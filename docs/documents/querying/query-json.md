# Querying for Raw JSON

Polecat can return documents as raw JSON strings without deserializing them into .NET objects.

## LoadJsonAsync

Load a single document as JSON by ID:

```cs
string? json = await session.LoadJsonAsync<User>(userId);
```

Returns `null` if the document doesn't exist.

## ToJsonArrayAsync

Convert a LINQ query result to a JSON array string:

```cs
string jsonArray = await session.Query<User>()
    .Where(x => x.Active)
    .ToJsonArrayAsync();

// Returns: [{"id":"...","firstName":"Alice",...},{"id":"...","firstName":"Bob",...}]
```

## Use Cases

Raw JSON queries are useful when:

- Serving JSON directly to HTTP responses without deserialization/serialization overhead
- Building APIs where the response format matches the stored document
- Streaming large result sets to clients
- Debugging or inspecting stored data
