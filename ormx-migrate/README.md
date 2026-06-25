# ormx-migrate

Language-agnostic ORMX migration utilities:

- Discover paired SQL migrations in **flat** (`{version}.up.sql` / `.down.sql`) or **folder** (`{version}/up.sql` / `down.sql`) layout
- DML helpers (`insert_rows`, `sql_literal`, `delete_from`)
- Shared `ormx_migrations` state-table DDL
- Static SQL validation before apply/rollback

Used by `ormx-core` (Python), `ormx-go-native`, `ormx-node-native`, and other SDKs — write the logic once, thin-wrap per language.
