# Exporting Schema Definition

Polecat can export the complete database schema as SQL scripts for review, version control, or manual deployment.

## ToDatabaseScript

Generate the complete DDL as a string:

```cs
var script = await store.Advanced.ToDatabaseScript();
Console.WriteLine(script);
```

This returns `CREATE TABLE` statements for all configured document tables and event store tables.

## WriteCreationScriptToFileAsync

Save the schema script to a file:

```cs
await store.Advanced.WriteCreationScriptToFileAsync("schema.sql");
```

## Use Cases

- **Code review** -- Include schema scripts in pull requests
- **Version control** -- Track schema changes over time
- **Manual deployment** -- Apply scripts to production databases manually
- **Documentation** -- Understand the database structure
- **DBA review** -- Allow database administrators to review before deployment

## Example Output

```sql
-- Polecat Schema Script

CREATE TABLE dbo.pc_streams (
    id uniqueidentifier NOT NULL PRIMARY KEY,
    type nvarchar(250) NULL,
    version int NOT NULL DEFAULT 0,
    ...
);

CREATE TABLE dbo.pc_events (
    seq_id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
    id uniqueidentifier NOT NULL,
    stream_id uniqueidentifier NOT NULL,
    ...
);

CREATE TABLE dbo.pc_event_progression (
    name nvarchar(250) NOT NULL PRIMARY KEY,
    last_seq_id bigint NOT NULL DEFAULT 0,
    ...
);

CREATE TABLE dbo.pc_doc_user (
    id uniqueidentifier NOT NULL PRIMARY KEY,
    data json NOT NULL,
    ...
);
```
