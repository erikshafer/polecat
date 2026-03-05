# Multi Stream Projections

Multi stream projections aggregate events from multiple streams into a single document.

## Defining a Multi Stream Projection

```cs
public class CustomerDashboard
{
    public Guid Id { get; set; }
    public int TotalOrders { get; set; }
    public decimal TotalSpent { get; set; }
    public DateTimeOffset? LastOrderDate { get; set; }
}

public class CustomerDashboardProjection : MultiStreamProjection<CustomerDashboard, Guid>
{
    public CustomerDashboardProjection()
    {
        // Route events to the correct aggregate by extracting the customer ID
        Identity<OrderCreated>(e => e.CustomerId);
        Identity<OrderShipped>(e => e.CustomerId);
        Identity<OrderCancelled>(e => e.CustomerId);
    }

    public static CustomerDashboard Create(OrderCreated e) =>
        new()
        {
            TotalOrders = 1,
            TotalSpent = e.Amount,
            LastOrderDate = DateTimeOffset.UtcNow
        };

    public void Apply(OrderCreated e, CustomerDashboard current)
    {
        current.TotalOrders++;
        current.TotalSpent += e.Amount;
        current.LastOrderDate = DateTimeOffset.UtcNow;
    }

    public void Apply(OrderCancelled e, CustomerDashboard current)
    {
        current.TotalOrders--;
        current.TotalSpent -= e.RefundAmount;
    }
}
```

## Registration

```cs
opts.Projections.Add<CustomerDashboardProjection>(ProjectionLifecycle.Async);
```

Multi stream projections can run inline or async, but async is typically recommended since they process events across streams.

## Event Routing

The `Identity<TEvent>()` method tells Polecat which aggregate document an event belongs to:

```cs
// Route by a property on the event
Identity<OrderCreated>(e => e.CustomerId);

// For string IDs
Identity<OrderCreated>(e => e.CustomerKey);
```

## ID Types

Multi stream projections support any ID type for the aggregate:

```cs
// Guid IDs
public class MyProjection : MultiStreamProjection<MyDoc, Guid> { }

// String IDs
public class MyProjection : MultiStreamProjection<MyDoc, string> { }

// Int IDs
public class MyProjection : MultiStreamProjection<MyDoc, int> { }
```
