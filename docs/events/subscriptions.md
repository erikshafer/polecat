# Event Subscriptions

Polecat supports event subscriptions for push-based processing of events as they are appended.

## ISubscription Interface

Implement `ISubscription` to process events:

```cs
public class OrderNotificationSubscription : SubscriptionBase
{
    public override async Task ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken ct)
    {
        foreach (var @event in page.Events)
        {
            if (@event.Data is OrderCreated created)
            {
                // Send notification, update external system, etc.
                await SendNotification(created);
            }
        }
    }
}
```

## Registering Subscriptions

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Projections.Subscribe(new OrderNotificationSubscription());
});
```

## How Subscriptions Work

Subscriptions are processed by the async daemon alongside projections:

1. The daemon tracks progression via `pc_event_progression`
2. Events are loaded in batches
3. Your subscription's `ProcessEventsAsync` is called for each batch
4. Progression is updated after successful processing

## Subscriptions vs Projections

| Feature | Subscription | Projection |
| :--- | :--- | :--- |
| Purpose | Side effects (notifications, external systems) | Read model construction |
| Output | Arbitrary | Documents or flat tables |
| Replay | May not be idempotent | Should be idempotent |
| Processing | Sequential batches | Sequential batches |

## SubscriptionBase

The `SubscriptionBase` class provides a convenient base with default implementations. Override `ProcessEventsAsync` to handle events.

::: tip
Unlike projections, subscriptions are intended for side effects like sending emails, updating external systems, or triggering workflows. They are not expected to be idempotent or replayable.
:::
