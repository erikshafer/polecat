# Bootstrapping Polecat

Polecat provides `AddPolecat()` extension methods on `IServiceCollection` for easy integration with .NET's dependency injection.

## Basic Registration

The simplest way to register Polecat:

```cs
builder.Services.AddPolecat(options =>
{
    options.Connection("Server=localhost;Database=myapp;User Id=sa;Password=YourStrong!Password;TrustServerCertificate=True");
});
```

## Registration Overloads

Polecat offers several `AddPolecat()` overloads:

```cs
// Connection string only
builder.Services.AddPolecat("Server=localhost;Database=myapp;...");

// Action-based configuration
builder.Services.AddPolecat(options =>
{
    options.Connection("...");
    options.DatabaseSchemaName = "myschema";
});

// Pre-built StoreOptions
var storeOptions = new StoreOptions();
storeOptions.Connection("...");
builder.Services.AddPolecat(storeOptions);

// Factory-based (access IServiceProvider)
builder.Services.AddPolecat(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var opts = new StoreOptions();
    opts.Connection(config.GetConnectionString("SqlServer")!);
    return opts;
});
```

## Registered Services

`AddPolecat()` registers the following services:

| Service | Lifetime | Description |
| :--- | :--- | :--- |
| `IDocumentStore` | Singleton | Main entry point, creates sessions |
| `ISessionFactory` | Singleton | Factory for creating sessions (default: lightweight) |
| `IDocumentSession` | Scoped | Read/write session with unit of work |
| `IQuerySession` | Scoped | Read-only session for queries |

## IConfigurePolecat

You can implement `IConfigurePolecat` to modularize your configuration:

```cs
public class MyPolecatConfig : IConfigurePolecat
{
    public void Configure(IServiceProvider services, StoreOptions options)
    {
        // Apply configuration here
    }
}
```

Register it before `AddPolecat()`:

```cs
builder.Services.AddSingleton<IConfigurePolecat, MyPolecatConfig>();
builder.Services.AddPolecat(options =>
{
    options.Connection("...");
});
```

## Session Factory

By default, Polecat creates lightweight sessions (no identity tracking). You can change this by providing a custom `ISessionFactory`:

```cs
builder.Services.AddPolecat(options =>
{
    options.Connection("...");
});
```

::: tip
Lightweight sessions are recommended for most use cases. Only use `IdentityMap` sessions when you need to ensure the same document instance is returned for repeated loads within a session.
:::
