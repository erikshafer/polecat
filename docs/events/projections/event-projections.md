# Event Projections

Event projections process individual events to create, modify, or delete documents. Unlike aggregate projections, they don't maintain per-stream state.

## Defining an Event Projection

```cs
public class AuditLogProjection : EventProjection
{
    public void Project(IEvent<OrderCreated> @event, IDocumentOperations ops)
    {
        ops.Store(new AuditEntry
        {
            Id = Guid.NewGuid(),
            Action = "OrderCreated",
            StreamId = @event.StreamId,
            Timestamp = @event.Timestamp,
            Details = $"Order created for {@@event.Data.Amount:C}"
        });
    }

    public void Project(IEvent<OrderCancelled> @event, IDocumentOperations ops)
    {
        ops.Store(new AuditEntry
        {
            Id = Guid.NewGuid(),
            Action = "OrderCancelled",
            StreamId = @event.StreamId,
            Timestamp = @event.Timestamp,
            Details = "Order was cancelled"
        });
    }
}
```

## Registration

```cs
opts.Projections.Add<AuditLogProjection>(ProjectionLifecycle.Inline);
// or
opts.Projections.Add<AuditLogProjection>(ProjectionLifecycle.Async);
```

## Use Cases

Event projections are ideal for:

- **Audit logs** -- Create a record for each significant event
- **Search indexes** -- Maintain denormalized documents for search
- **Notifications** -- Create notification records per event
- **Cross-cutting concerns** -- Track metrics, analytics, or compliance data

## Accessing Session Operations

The `IDocumentOperations` parameter gives you full access to document operations:

```cs
public void Project(IEvent<UserDeactivated> @event, IDocumentOperations ops)
{
    // Store new documents
    ops.Store(new DeactivationRecord { ... });

    // Delete documents
    ops.Delete<ActiveUser>(@event.Data.UserId);

    // Patch existing documents
    ops.Patch<UserStats>(@event.Data.UserId)
        .Set(x => x.IsActive, false);
}
```
