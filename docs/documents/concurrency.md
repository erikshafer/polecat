# Optimistic Concurrency

Polecat supports two forms of optimistic concurrency control to prevent lost updates.

## Guid-Based Versioning (IVersioned)

Each save generates a new Guid version. Concurrent modifications are detected when the expected version doesn't match:

```cs
public class Order : IVersioned
{
    public Guid Id { get; set; }
    public Guid Version { get; set; }
    public string Description { get; set; } = "";
}
```

Usage:

```cs
// Load and modify
var order = await session.LoadAsync<Order>(orderId);
// order.Version is automatically populated

order.Description = "Updated";
session.Store(order);
await session.SaveChangesAsync();
// order.Version is now a new Guid

// If another session modified the order between load and save,
// SaveChangesAsync throws ConcurrencyException
```

### UpdateExpectedVersion

Explicitly set the expected version for concurrency checks:

```cs
session.UpdateExpectedVersion(order, expectedGuidVersion);
```

## Numeric Revisions (IRevisioned)

An integer revision counter that increments on each save:

```cs
public class Order : IRevisioned
{
    public Guid Id { get; set; }
    public int Version { get; set; }
    public string Description { get; set; } = "";
}
```

Usage:

```cs
var order = await session.LoadAsync<Order>(orderId);
// order.Version == 1 (after first save)

order.Description = "Updated";
session.Store(order);
await session.SaveChangesAsync();
// order.Version == 2
```

### UpdateRevision

Explicitly set the expected revision:

```cs
session.UpdateRevision(order, expectedRevision: 3);
```

## Configuration

### Auto-Detection

Polecat automatically detects concurrency mode from interfaces:

- Implements `IVersioned` → Guid-based versioning
- Implements `IRevisioned` → Numeric revisions

### Manual Configuration

```cs
opts.Policies.ForDocument<Order>(mapping =>
{
    mapping.UseOptimisticConcurrency = true; // Guid-based
    // OR
    mapping.UseNumericRevisions = true; // Integer-based
});
```

::: warning
`UseOptimisticConcurrency` and `UseNumericRevisions` are mutually exclusive. Choose one per document type.
:::

## ConcurrencyException

When a concurrent modification is detected, Polecat throws `ConcurrencyException` (from JasperFx):

```cs
try
{
    await session.SaveChangesAsync();
}
catch (ConcurrencyException ex)
{
    // Handle the conflict -- reload, merge, or notify the user
}
```

## How It Works

- **First save** (version is zero/empty): No concurrency check -- the document is inserted
- **Subsequent saves**: The MERGE/UPDATE statement includes a version check in its WHERE clause
- **Version sync**: After a successful save, the new version is read back from the database via OUTPUT clause and synced to the in-memory document
- **LINQ queries**: Version columns are included in SELECT, and version properties are synced on deserialization
