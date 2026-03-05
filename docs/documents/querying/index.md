# Querying Documents

Polecat provides several ways to query documents from SQL Server.

## Loading by ID

The simplest way to retrieve a document:

```cs
var user = await session.LoadAsync<User>(userId);
```

See [Loading Documents by Id](/documents/querying/byid) for more details.

## LINQ Queries

Full LINQ support for complex queries:

```cs
var users = await session.Query<User>()
    .Where(x => x.LastName == "Smith")
    .OrderBy(x => x.FirstName)
    .ToListAsync();
```

See [Querying with LINQ](/documents/querying/linq/) for more details.

## Raw JSON Queries

Load documents as raw JSON strings:

```cs
var json = await session.LoadJsonAsync<User>(userId);
var jsonArray = await session.Query<User>().ToJsonArrayAsync();
```

See [Querying for Raw JSON](/documents/querying/query-json) for more details.

## Batched Queries

Execute multiple queries in a single database round-trip:

```cs
var batch = session.CreateBatchQuery();
var userTask = batch.Load<User>(userId);
var ordersTask = batch.Query<Order>().Where(x => x.Status == "Active").ToList();
await batch.Execute();

var user = await userTask;
var orders = await ordersTask;
```

See [Batched Queries](/documents/querying/batched-queries) for more details.

## SQL Preview

Preview the generated SQL for any LINQ query:

```cs
var sql = session.Query<User>()
    .Where(x => x.LastName == "Smith")
    .ToSql();
```
