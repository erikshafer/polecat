# Querying Events

Polecat provides several ways to read events and aggregate state from streams.

## FetchStreamAsync

Load all events for a stream:

```cs
var events = await session.Events.FetchStreamAsync(streamId);

foreach (var @event in events)
{
    Console.WriteLine($"[{@event.Version}] {@event.EventTypeName}: {@event.Data}");
}
```

Events are returned in version order. Archived streams are automatically excluded.

## AggregateStreamAsync

Replay events to build the current aggregate state:

```cs
var party = await session.Events.AggregateStreamAsync<QuestParty>(streamId);
```

If snapshots are enabled, Polecat loads the latest snapshot and only replays events after the snapshot version.

### With Version Cap

Replay only up to a specific version:

```cs
var partyAtV3 = await session.Events.AggregateStreamAsync<QuestParty>(streamId, version: 3);
```

### With Timestamp Cap

Replay only events before a specific timestamp:

```cs
var partyAtTime = await session.Events.AggregateStreamAsync<QuestParty>(streamId,
    timestamp: DateTimeOffset.Parse("2024-01-15"));
```

## FetchForWriting

Load an aggregate with its current version for optimistic concurrency:

```cs
var stream = await session.Events.FetchForWriting<QuestParty>(streamId);

Console.WriteLine(stream.Aggregate.Name);      // Current state
Console.WriteLine(stream.CurrentVersion);       // Current version

stream.AppendOne(new MembersDeparted(...));
await session.SaveChangesAsync();
```

## FetchForExclusiveWriting

Load with a pessimistic lock (SQL Server `UPDLOCK HOLDLOCK`):

```cs
var stream = await session.Events.FetchForExclusiveWriting<QuestParty>(streamId);
// Row is locked until transaction completes
```

## IEvent Interface

Each event returned from `FetchStreamAsync` implements `IEvent`:

| Property | Type | Description |
| :--- | :--- | :--- |
| `Id` | `Guid` | Unique event ID |
| `Sequence` | `long` | Global sequence number |
| `Version` | `int` | Position within the stream |
| `Data` | `object` | Deserialized event body |
| `EventTypeName` | `string` | Event type name (snake_case) |
| `Timestamp` | `DateTimeOffset` | When recorded |
| `StreamId` / `StreamKey` | `Guid` / `string` | Stream identifier |
| `TenantId` | `string` | Tenant identifier |
| `CorrelationId` | `string?` | Correlation ID |
| `CausationId` | `string?` | Causation ID |
| `Headers` | `Dictionary` | Custom headers |

## QueryForNonStaleData

Wait for async projections to catch up before querying:

```cs
var orders = await session.Query<OrderSummary>()
    .QueryForNonStaleData()
    .Where(x => x.Status == "Active")
    .ToListAsync();
```

With a custom timeout:

```cs
var orders = await session.Query<OrderSummary>()
    .QueryForNonStaleData(TimeSpan.FromSeconds(10))
    .Where(x => x.Status == "Active")
    .ToListAsync();
```
