# Loading Documents by Id

The most direct way to load documents is by their ID.

## LoadAsync

Load a single document by its ID:

```cs
var user = await session.LoadAsync<User>(userId);
```

Returns `null` if the document doesn't exist.

## LoadManyAsync

Load multiple documents by their IDs in a single query:

```cs
var users = await session.LoadManyAsync<User>(userId1, userId2, userId3);
```

Returns a list containing only the documents that were found. Missing documents are silently omitted.

## Identity Map Behavior

With `DocumentTracking.IdentityMap`, repeated loads of the same document return the same instance:

```cs
await using var session = store.OpenSession(DocumentTracking.IdentityMap);

var user1 = await session.LoadAsync<User>(userId);
var user2 = await session.LoadAsync<User>(userId);

// user1 and user2 are the same instance
Assert.Same(user1, user2);
```

With lightweight sessions, each load returns a new instance.

## Strongly Typed IDs

Loading with strongly typed IDs works the same way:

```cs
public record struct UserId(Guid Value);

var user = await session.LoadAsync<User>(new UserId(guid));
```

## Soft Delete Filtering

When soft deletes are enabled, `LoadAsync` automatically excludes soft-deleted documents:

```cs
// Returns null if the document was soft-deleted
var order = await session.LoadAsync<Order>(orderId);
```

## JSON Loading

Load a document as a raw JSON string:

```cs
string? json = await session.LoadJsonAsync<User>(userId);
```
