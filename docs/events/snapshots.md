# Event Snapshots

Polecat can store aggregate snapshots alongside stream metadata to optimize event replay performance.

## How Snapshots Work

Instead of replaying all events from the beginning of a stream, Polecat can:

1. Load the latest snapshot from the `pc_streams` table
2. Only replay events that occurred after the snapshot version
3. Return the fully hydrated aggregate

This significantly reduces replay time for streams with many events.

## Inline Snapshots

Register a snapshot that updates in the same transaction as event appending:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline);
});
```

When events are appended, the projection runs inline and the resulting aggregate state is stored as a JSON snapshot in the `pc_streams.snapshot` column.

## Snapshot Storage

Snapshots are stored directly in the `pc_streams` table:

| Column | Type | Description |
| :--- | :--- | :--- |
| `snapshot` | `json` | Serialized aggregate state |
| `snapshot_version` | `int` | Version at which snapshot was taken |

## Using Snapshots with AggregateStreamAsync

When snapshots are available, `AggregateStreamAsync` automatically uses them:

```cs
var party = await session.Events.AggregateStreamAsync<QuestParty>(streamId);
// If a snapshot exists at version 50 and stream is at version 75,
// only events 51-75 are replayed on top of the snapshot
```

### Snapshot Behavior

- If no snapshot exists, all events are replayed from the beginning
- If the snapshot version matches the stream version, the snapshot is returned directly
- If the snapshot version is less than the requested version, events after the snapshot are replayed
- If a version cap is specified that's less than the snapshot version, the snapshot is skipped and all events are replayed

## Async Snapshots

Snapshots can also be updated by the async daemon:

```cs
opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
```

This updates snapshots in the background as the async daemon processes events.
