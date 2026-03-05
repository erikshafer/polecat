# Querying Documents with LINQ

Polecat provides a custom LINQ provider that translates .NET LINQ queries into SQL Server queries against the JSON document data.

## Basic Queries

```cs
// Simple filter
var smiths = await session.Query<User>()
    .Where(x => x.LastName == "Smith")
    .ToListAsync();

// With ordering
var sorted = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .ThenBy(x => x.FirstName)
    .ToListAsync();

// First or default
var first = await session.Query<User>()
    .FirstOrDefaultAsync(x => x.Email == "alice@example.com");
```

## Aggregates

```cs
var count = await session.Query<User>().CountAsync();
var any = await session.Query<User>().AnyAsync(x => x.Internal);
```

## Paging

```cs
var page = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .Skip(20)
    .Take(10)
    .ToListAsync();
```

Or use the built-in paging support:

```cs
var pagedList = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .ToPagedListAsync(pageNumber: 2, pageSize: 10);
```

See [Paging](/documents/querying/linq/paging) for more details.

## How It Works

LINQ queries are translated to SQL using `JSON_VALUE()` to extract properties from the JSON document:

```sql
SELECT data FROM pc_doc_user
WHERE JSON_VALUE(data, '$.lastName') = @p0
ORDER BY JSON_VALUE(data, '$.firstName')
```

The LINQ provider supports:

- Equality and comparison operators
- String operations (Contains, StartsWith, EndsWith)
- Boolean logic (And, Or, Not)
- Null checks
- Collection operations (Any, All, Contains)
- Arithmetic operations
- Nested property access

See [Supported LINQ Operators](/documents/querying/linq/operators) for a complete list.
