# Migrations

## Authoring

Create a new revision with:

```bash
uv run alembic revision -m "describe change"
```

Then write `upgrade()` and `downgrade()` by hand in the generated revision file.
do not run `alembic revision --autogenerate` — it is forbidden by §B of the pms-correctness-bundle-v1 spec.

## Applying

Point `DATABASE_URL` at the target database, then apply the current head:

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/pms_dev
uv run alembic upgrade head
```

For integration runs that already export `PMS_TEST_DATABASE_URL`, mirror it into
`DATABASE_URL` before invoking Alembic:

```bash
export DATABASE_URL="$PMS_TEST_DATABASE_URL"
uv run alembic upgrade head
```

## Rolling back

To roll back the current database to base:

```bash
uv run alembic downgrade base
```

Use the same `DATABASE_URL` you used for the upgrade so the downgrade targets
the intended database.

## Why we don't autogenerate

`schema.sql` remains the reference artifact for the committed schema, but the
runtime contract is Alembic revisions. Manual migrations keep review focused on
the exact DDL being introduced and avoid accidental drift from reflection-based
autogeneration.
