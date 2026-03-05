# Live Aggregations

Live aggregation replays events on demand to build the current aggregate state without persisting a read model.

## Basic Usage

```cs
var order = await session.Events.AggregateStreamAsync<OrderSummary>(streamId);
```

This loads all events for the stream and applies them through the aggregate's `Create` and `Apply` methods.

## When to Use Live Aggregation

Live aggregation is best for:

- **Infrequently accessed aggregates** -- No need to maintain a persistent read model
- **Always-current state** -- No staleness, always reflects the latest events
- **Streams with few events** -- Low replay cost
- **Testing and debugging** -- Verify aggregate behavior

## Version Cap

Replay only up to a specific version:

```cs
var orderAtV5 = await session.Events.AggregateStreamAsync<OrderSummary>(streamId, version: 5);
```

## Timestamp Cap

Replay only events before a specific time:

```cs
var orderAtTime = await session.Events.AggregateStreamAsync<OrderSummary>(
    streamId,
    timestamp: DateTimeOffset.Parse("2024-06-15")
);
```

## With Snapshots

If a snapshot exists for the stream, `AggregateStreamAsync` automatically uses it to optimize replay:

1. Load the snapshot from `pc_streams`
2. Only replay events after the snapshot version
3. Return the hydrated aggregate

See [Snapshots](/events/snapshots) for details on configuring snapshot storage.

## Live vs Inline vs Async

| Strategy | Consistency | Storage | Performance |
| :--- | :--- | :--- | :--- |
| Live | Always current | None | Replay cost per read |
| Inline | Always current | Document table | Write overhead |
| Async | Eventually consistent | Document table | Background processing |
