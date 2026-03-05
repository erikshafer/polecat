# Schema Migrations

Polecat uses Weasel.SqlServer for diff-based schema migrations.

## Auto-Create Behavior

By default (`AutoCreate.CreateOrUpdate`), Polecat automatically creates and updates database schema on application startup:

```cs
opts.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
```

### What Auto-Create Does

- Creates document tables on first use
- Adds new columns when document configuration changes (e.g., enabling soft deletes)
- Creates event store tables on startup
- Creates the HiLo table on first use
- **Never drops columns or tables**

### What Auto-Create Does NOT Do

- Drop existing columns
- Rename columns
- Modify column types
- Delete data

## Disabling Auto-Create

For production environments:

```cs
opts.AutoCreateSchemaObjects = AutoCreate.None;
```

When auto-create is disabled, you're responsible for ensuring the database schema matches your configuration. Use the [schema export](/schema/exporting) feature to generate DDL scripts.

## Migration Flow

On startup, Polecat's migration flow:

1. Compare desired schema (from configuration) against actual database schema
2. Generate DDL for differences (new tables, new columns)
3. Execute DDL within a transaction
4. Run initial data seeding if configured

## Weasel.SqlServer

All schema management is delegated to Weasel.SqlServer, which provides:

- **Diff-based migrations** -- Only applies changes that are needed
- **Safe operations** -- Never drops columns or tables
- **Idempotent** -- Safe to run multiple times
- **Transaction safety** -- Schema changes are transactional
