# Batched Queries

Polecat supports batching multiple queries into a single database round-trip using `IBatchedQuery`.

## Creating a Batch

```cs
var batch = session.CreateBatchQuery();
```

## Batch Operations

### Load by ID

```cs
var userTask = batch.Load<User>(userId);
```

### Load Many

```cs
var usersTask = batch.LoadMany<User>(userId1, userId2, userId3);
```

### LINQ Query

```cs
var ordersTask = batch.Query<Order>()
    .Where(x => x.Status == "Active")
    .ToList();
```

## Executing the Batch

```cs
await batch.Execute();

// Now resolve the results
var user = await userTask;
var users = await usersTask;
var orders = await ordersTask;
```

All queries in the batch execute in a single database call, significantly reducing latency when you need to load multiple independent pieces of data.

## Query Plans

For reusable query specifications, implement `IBatchQueryPlan<T>`:

```cs
public class ActiveOrdersPlan : QueryListPlan<Order>
{
    protected override IQueryable<Order> Query(IQuerySession session)
    {
        return session.Query<Order>().Where(x => x.Status == "Active");
    }
}

// Use in a batch
var ordersTask = batch.QueryByPlan(new ActiveOrdersPlan());
await batch.Execute();
var orders = await ordersTask;
```

Query plans can also be used independently:

```cs
var orders = await session.QueryByPlanAsync(new ActiveOrdersPlan());
```
