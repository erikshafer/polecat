# Event Multi-Tenancy

Polecat supports multi-tenancy in the event store through conjoined tenancy (shared tables) or separate database tenancy.

## Conjoined Tenancy

Enable tenant isolation within shared event tables:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Events.TenancyStyle = TenancyStyle.Conjoined;
});
```

With conjoined tenancy:

- All events include a `tenant_id` column
- Stream queries automatically filter by tenant
- Event appending records the session's tenant ID
- The async daemon processes events per-tenant

### Using Tenanted Events

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    TenantId = "tenant-abc"
});

// Events are stored with tenant_id = "tenant-abc"
session.Events.StartStream<Order>(
    new OrderCreated(100m, "Widget")
);
await session.SaveChangesAsync();

// Only loads events for "tenant-abc"
var order = await session.Events.AggregateStreamAsync<Order>(streamId);
```

## Separate Database Tenancy

Each tenant gets its own database with independent event stores:

```cs
var store = DocumentStore.For(opts =>
{
    opts.MultiTenantedDatabases(databases =>
    {
        databases.AddSingleTenantDatabase("Server=localhost;Database=events_tenant_a;...", "tenant-a");
        databases.AddSingleTenantDatabase("Server=localhost;Database=events_tenant_b;...", "tenant-b");
    });
});
```

With separate database tenancy:

- Each tenant has completely isolated event data
- The async daemon runs independently per database
- Schema management is independent per database

## Default Tenant

When no tenant ID is specified, events are stored with `tenant_id = 'DEFAULT'`. In single-tenant mode, all queries use this default value.
