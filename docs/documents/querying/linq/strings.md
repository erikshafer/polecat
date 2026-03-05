# Searching on String Fields

Polecat's LINQ provider translates .NET string methods into SQL Server JSON path queries.

## Contains

```cs
var results = await session.Query<User>()
    .Where(x => x.LastName.Contains("son"))
    .ToListAsync();
```

## StartsWith

```cs
var results = await session.Query<User>()
    .Where(x => x.LastName.StartsWith("Sm"))
    .ToListAsync();
```

## EndsWith

```cs
var results = await session.Query<User>()
    .Where(x => x.Email.EndsWith("@example.com"))
    .ToListAsync();
```

## Equals

```cs
var results = await session.Query<User>()
    .Where(x => x.Email.Equals("admin@example.com"))
    .ToListAsync();
```

## IsNullOrEmpty

```cs
var results = await session.Query<User>()
    .Where(x => !string.IsNullOrEmpty(x.Email))
    .ToListAsync();
```

## Case Sensitivity

SQL Server string comparisons follow the database collation by default. For case-insensitive searches, configure your database collation accordingly.
