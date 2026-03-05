# Archiving Streams

Polecat supports archiving event streams to logically remove them from active queries without permanently deleting the data.

## Archiving a Stream

```cs
session.Events.ArchiveStream(streamId);
await session.SaveChangesAsync();
```

This sets `is_archived = 1` on both the `pc_streams` and `pc_events` tables for the stream.

## Effects of Archiving

When a stream is archived:

- `FetchStreamAsync` excludes the archived stream
- The async daemon's event loader skips archived events
- Attempting to append to an archived stream throws `InvalidStreamException`

## Unarchiving a Stream

Restore an archived stream:

```cs
session.Events.UnArchiveStream(streamId);
await session.SaveChangesAsync();
```

This sets `is_archived = 0` on both tables, making the stream active again.

## Tombstoning (Hard Delete)

For permanent removal of a stream and all its events:

```cs
session.Events.TombstoneStream(streamId);
await session.SaveChangesAsync();
```

::: warning
Tombstoning permanently `DELETE`s the stream record and all associated events from the database. This cannot be undone.
:::

Tombstoning works with both Guid and string stream IDs.

## Archiving vs Tombstoning

| Operation | Reversible | Data Preserved | Use Case |
| :--- | :--- | :--- | :--- |
| Archive | Yes | Yes | Soft removal, compliance holds |
| Tombstone | No | No | GDPR right to erasure, cleanup |
