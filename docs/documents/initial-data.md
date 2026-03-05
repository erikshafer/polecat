# Initial Baseline Data

Polecat can automatically seed data on application startup using the `IInitialData` interface.

## IInitialData Interface

```cs
public class SeedUsers : IInitialData
{
    public async Task Populate(IDocumentStore store, CancellationToken ct)
    {
        await using var session = store.LightweightSession();

        session.Store(new User { FirstName = "Admin", LastName = "User" });
        session.Store(new User { FirstName = "Test", LastName = "User" });

        await session.SaveChangesAsync(ct);
    }
}
```

Register in configuration:

```cs
opts.InitialData.Add(new SeedUsers());
```

## Lambda-Based Seeding

For simple cases, use a lambda:

```cs
opts.InitialData.Add(async (store, ct) =>
{
    await using var session = store.LightweightSession();
    session.Store(new DefaultSettings { Id = "default", Theme = "dark" });
    await session.SaveChangesAsync(ct);
});
```

## Execution Order

Initial data runs after schema migration on application startup, before the application starts accepting requests. All registered `IInitialData` implementations are executed in the order they were registered.

::: tip
Initial data seeding runs via the `PolecatActivator` which is triggered by the .NET host's startup pipeline.
:::
