# Integration Testing

Polecat provides patterns for writing integration tests against a real SQL Server database.

## Test Infrastructure

### Docker Compose

Use Docker Compose to run SQL Server 2025 for testing:

```yaml
services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2025-CU1-ubuntu-24.04
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "Polecat#Dev2025"
    ports:
      - "11433:1433"
```

### Connection String

```cs
private const string ConnectionString =
    "Server=localhost,11433;User Id=sa;Password=Polecat#Dev2025;Database=polecat_testing;TrustServerCertificate=True";
```

## IntegrationContext Base Class

Create a base class for your integration tests:

```cs
public abstract class IntegrationContext : IAsyncLifetime
{
    protected IDocumentStore Store { get; private set; } = null!;

    protected virtual void ConfigureStore(StoreOptions opts)
    {
        // Override to customize store configuration
    }

    public async Task InitializeAsync()
    {
        Store = DocumentStore.For(opts =>
        {
            opts.Connection(ConnectionString);
            ConfigureStore(opts);
        });

        // Clean slate for each test
        await Store.Advanced.CleanAllDocumentsAsync();
        await Store.Advanced.CleanAllEventDataAsync();
    }

    public async Task DisposeAsync()
    {
        await Store.DisposeAsync();
    }

    protected IDocumentSession OpenSession() => Store.LightweightSession();
    protected IQuerySession QuerySession() => Store.QuerySession();
}
```

## Example Test

```cs
public class UserStorageTests : IntegrationContext
{
    [Fact]
    public async Task can_store_and_load_document()
    {
        var user = new User { FirstName = "Alice", LastName = "Smith" };

        await using (var session = OpenSession())
        {
            session.Store(user);
            await session.SaveChangesAsync();
        }

        await using (var session = QuerySession())
        {
            var loaded = await session.LoadAsync<User>(user.Id);
            loaded.ShouldNotBeNull();
            loaded.FirstName.ShouldBe("Alice");
        }
    }
}
```

## Event Sourcing Tests

```cs
public class EventStoreTests : IntegrationContext
{
    [Fact]
    public async Task can_append_and_aggregate()
    {
        Guid streamId;

        await using (var session = OpenSession())
        {
            streamId = session.Events.StartStream<QuestParty>(
                new QuestStarted("Test Quest"),
                new MembersJoined("Start", ["Alice", "Bob"])
            );
            await session.SaveChangesAsync();
        }

        await using (var session = QuerySession())
        {
            var party = await session.Events.AggregateStreamAsync<QuestParty>(streamId);
            party.ShouldNotBeNull();
            party.Members.Count.ShouldBe(2);
        }
    }
}
```

## Cleaning Data Between Tests

```cs
// Clean all documents
await store.Advanced.CleanAllDocumentsAsync();

// Clean specific document type
await store.Advanced.CleanAsync<User>();

// Clean all event data
await store.Advanced.CleanAllEventDataAsync();
```

## Testing Async Projections

For async daemon tests, use `CatchUpAsync` to wait for projections:

```cs
[Fact]
public async Task async_projection_catches_up()
{
    await using var session = OpenSession();
    session.Events.StartStream<Order>(new OrderCreated(100m, "Acme"));
    await session.SaveChangesAsync();

    // Wait for the daemon to process
    await Store.WaitForNonStaleProjectionDataAsync(TimeSpan.FromSeconds(10));

    await using var query = QuerySession();
    var summary = await query.LoadAsync<OrderSummary>(streamId);
    summary.ShouldNotBeNull();
}
```
