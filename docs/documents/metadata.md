# Polecat Metadata

Polecat can automatically track metadata on your documents through several built-in interfaces.

## ITracked

Implement `ITracked` to have correlation, causation, and user tracking automatically synced:

```cs
public class Order : ITracked
{
    public Guid Id { get; set; }
    public string Description { get; set; } = "";

    // ITracked members - auto-synced from session
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? LastModifiedBy { get; set; }
}
```

Set tracking values on the session:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    CorrelationId = "request-123",
    CausationId = "command-456",
    LastModifiedBy = "user@example.com"
});

session.Store(new Order { Description = "New order" });
await session.SaveChangesAsync();
// Order.CorrelationId will be "request-123"
```

## ITenanted

Implement `ITenanted` to have the tenant ID automatically synced from the session:

```cs
public class Order : ITenanted
{
    public Guid Id { get; set; }
    public string TenantId { get; set; } = "";
}
```

See [Multi-Tenancy](/configuration/multitenancy) for more details.

## ISoftDeleted

Implement `ISoftDeleted` for automatic soft delete tracking:

```cs
public class Order : ISoftDeleted
{
    public Guid Id { get; set; }
    public bool Deleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
}
```

See [Deleting Documents](/documents/deletes#soft-deletes) for more details.

## IVersioned

Implement `IVersioned` for Guid-based optimistic concurrency:

```cs
public class Order : IVersioned
{
    public Guid Id { get; set; }
    public Guid Version { get; set; }
}
```

See [Optimistic Concurrency](/documents/concurrency) for more details.

## Built-in Metadata Columns

Every document table includes these metadata columns automatically:

| Column | Description |
| :--- | :--- |
| `last_modified` | Updated to `SYSDATETIMEOFFSET()` on every save |
| `created` | Set to `SYSDATETIMEOFFSET()` on first insert |
| `type` | Short type discriminator |
| `dotnet_type` | Full .NET assembly-qualified type name |

## Metadata LINQ Extensions

Query documents by their metadata:

```cs
// Find documents modified since a given time
var recent = await session.Query<Order>()
    .Where(x => x.ModifiedSince(DateTimeOffset.UtcNow.AddHours(-1)))
    .ToListAsync();

// Find documents modified before a given time
var old = await session.Query<Order>()
    .Where(x => x.ModifiedBefore(DateTimeOffset.UtcNow.AddDays(-30)))
    .ToListAsync();
```
