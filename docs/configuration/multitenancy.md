# Multi-Tenancy with Database per Tenant

Polecat supports multiple multi-tenancy strategies for isolating data between tenants.

## Tenancy Styles

### Single Tenant (Default)

All data lives in one set of tables with no tenant isolation:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    // This is the default -- no tenant isolation
});
```

### Conjoined Tenancy

All tenants share the same database and tables, but data is isolated by a `tenant_id` column:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // Enable conjoined tenancy for events
    opts.Events.TenancyStyle = TenancyStyle.Conjoined;
});
```

With conjoined tenancy:

- All document tables get a `tenant_id` column
- Document primary keys become composite: `(tenant_id, id)`
- All queries automatically filter by the session's tenant ID
- Event streams are isolated per tenant

Specify the tenant when creating a session:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    TenantId = "tenant-abc"
});
```

See [Multi-Tenanted Documents](/documents/multi-tenancy) and [Event Multi-Tenancy](/events/multitenancy) for more details.

### Separate Database Tenancy

Each tenant gets their own isolated SQL Server database:

```cs
var store = DocumentStore.For(opts =>
{
    opts.MultiTenantedDatabases(databases =>
    {
        databases.AddSingleTenantDatabase("Server=localhost;Database=tenant_a;...", "tenant-a");
        databases.AddSingleTenantDatabase("Server=localhost;Database=tenant_b;...", "tenant-b");
    });
});
```

With separate database tenancy:

- Each tenant has completely isolated data
- Schema management runs independently per database
- Sessions automatically route to the correct database based on tenant ID
- The async daemon runs independently per tenant database

## Setting the Tenant ID

The tenant ID is set when opening a session:

```cs
// Via SessionOptions
await using var session = store.LightweightSession(new SessionOptions
{
    TenantId = "my-tenant"
});
```

::: warning
If no tenant ID is specified, Polecat uses `"DEFAULT"` as the tenant ID. In conjoined tenancy mode, this means documents and events will be stored with `tenant_id = 'DEFAULT'`.
:::

## ITenanted Interface

Documents that implement `ITenanted` will have their `TenantId` property automatically synced from the session:

```cs
public class Order : ITenanted
{
    public Guid Id { get; set; }
    public string TenantId { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
}
```
