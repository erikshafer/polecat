# ProjectLatest — Include Pending Events

`ProjectLatest<T>()` returns the projected state of an aggregate including any events that have been
appended in the current session but not yet committed. This eliminates the need for a forced
`SaveChangesAsync()` + `FetchLatest()` round-trip.

## API

```csharp
// On IDocumentSession.Events (IEventOperations)
ValueTask<T?> ProjectLatest<T>(Guid id, CancellationToken cancellation = default);
ValueTask<T?> ProjectLatest<T>(string key, CancellationToken cancellation = default);
```

## Example

```csharp
await using var session = store.LightweightSession();

session.Events.StartStream(streamId,
    new ReportCreated("Q1 Report"),
    new SectionAdded("Revenue"),
    new SectionAdded("Costs")
);

// Get the projected state WITHOUT saving first
var report = await session.Events.ProjectLatest<Report>(streamId);
// report.Title == "Q1 Report"
// report.SectionCount == 2

// SaveChangesAsync happens later
await session.SaveChangesAsync();
```

## Behavior

1. Fetches the current committed aggregate state from the database
2. Finds any pending (uncommitted) events for that stream in the current session
3. Applies the pending events on top using the aggregate's Apply/Create methods
4. Returns the result

When no pending events exist, `ProjectLatest` behaves identically to `FetchLatest`.
