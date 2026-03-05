# Flat Table Projections

Flat table projections write events directly to SQL Server tables with defined columns, rather than as JSON documents. This is ideal for reporting and analytics scenarios.

## Defining a Flat Table Projection

```cs
public class OrderFlatTableProjection : FlatTableProjection
{
    public OrderFlatTableProjection() : base("order_history")
    {
        // Define table columns
        Table.AddColumn<Guid>("id").AsPrimaryKey();
        Table.AddColumn<Guid>("stream_id");
        Table.AddColumn<string>("status");
        Table.AddColumn<decimal>("amount");
        Table.AddColumn<DateTimeOffset>("event_time");

        // Map events to columns
        Project<OrderCreated>(map =>
        {
            map.Map(e => e.Amount, "amount");
            map.SetValue("status", "Created");
        });

        Project<OrderShipped>(map =>
        {
            map.SetValue("status", "Shipped");
        });

        // Delete rows on certain events
        Delete<OrderCancelled>();
    }
}
```

## Registration

```cs
opts.Projections.Add<OrderFlatTableProjection>(ProjectionLifecycle.Async);
```

## Column Mapping

### Map Event Properties

```cs
map.Map(e => e.Amount, "amount");
map.Map(e => e.CustomerName, "customer_name");
```

### Set Static Values

```cs
map.SetValue("status", "Shipped");
map.SetValue("processed", true);
```

## SQL Generation

Flat table projections use SQL Server `MERGE` statements for upsert behavior, ensuring idempotent processing.

## Use Cases

- **Reporting tables** -- Denormalized data for BI tools
- **Analytics** -- Pre-computed metrics per event
- **Audit trails** -- Structured event history in relational format
- **Integration** -- Data accessible by non-.NET systems via standard SQL

::: tip
Flat table projections bypass the document storage layer entirely, writing directly to user-defined SQL tables. This makes the data accessible to any SQL tool without understanding JSON document structure.
:::
