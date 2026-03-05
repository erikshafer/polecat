# Deleting Documents

Polecat supports both hard deletes (permanent removal) and soft deletes (logical deletion).

## Hard Deletes

By default, `Delete()` performs a permanent deletion:

```cs
// Delete by ID
session.Delete<User>(userId);

// Delete by document instance
session.Delete(user);

// Delete by predicate
session.DeleteWhere<User>(x => x.Internal == true);

await session.SaveChangesAsync();
```

## Soft Deletes

Soft deletes mark documents as deleted without removing them from the database. Enable soft deletes in one of three ways:

### Via Attribute

```cs
[SoftDeleted]
public class Order
{
    public Guid Id { get; set; }
    public string Description { get; set; } = "";
}
```

### Via Interface

```cs
public class Order : ISoftDeleted
{
    public Guid Id { get; set; }
    public bool Deleted { get; set; }
    public DateTimeOffset? DeletedAt { get; set; }
}
```

### Via Policy

```cs
// For a specific type
opts.Policies.ForDocument<Order>(mapping =>
{
    mapping.DeleteStyle = DeleteStyle.SoftDelete;
});

// For all document types
opts.Policies.AllDocumentsSoftDeleted();
```

### How Soft Deletes Work

When soft deletes are enabled:

- `Delete()` sets `is_deleted = 1` and `deleted_at = SYSDATETIMEOFFSET()` in the database
- If the document implements `ISoftDeleted`, the in-memory properties are also updated
- All queries automatically filter out soft-deleted documents
- `HardDelete()` still performs a permanent removal

### Querying Soft-Deleted Documents

LINQ extensions allow querying deleted documents:

```cs
// Include deleted documents in results
var all = await session.Query<Order>()
    .Where(x => x.MaybeDeleted())
    .ToListAsync();

// Only return deleted documents
var deleted = await session.Query<Order>()
    .Where(x => x.IsDeleted())
    .ToListAsync();

// Deleted since a specific time
var recentlyDeleted = await session.Query<Order>()
    .Where(x => x.DeletedSince(cutoff))
    .ToListAsync();

// Deleted before a specific time
var oldDeleted = await session.Query<Order>()
    .Where(x => x.DeletedBefore(cutoff))
    .ToListAsync();
```

### Undoing Soft Deletes

Restore soft-deleted documents:

```cs
session.UndoDeleteWhere<Order>(x => x.Description == "Restore me");
await session.SaveChangesAsync();
```

## Hard Delete (Force)

Even when soft deletes are enabled, you can force a permanent deletion:

```cs
session.HardDelete<Order>(orderId);
await session.SaveChangesAsync();
```
