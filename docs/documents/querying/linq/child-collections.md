# Querying within Child Collections

Polecat supports querying documents based on conditions within nested collections stored in the JSON data.

## Any

Check if a child collection has any elements matching a condition:

```cs
public class Order
{
    public Guid Id { get; set; }
    public List<OrderItem> Items { get; set; } = new();
}

public class OrderItem
{
    public string ProductName { get; set; } = "";
    public int Quantity { get; set; }
}

// Find orders that have any item with quantity > 10
var orders = await session.Query<Order>()
    .Where(x => x.Items.Any(i => i.Quantity > 10))
    .ToListAsync();
```

## Contains

Check if a simple collection contains a value:

```cs
public class User
{
    public Guid Id { get; set; }
    public List<string> Tags { get; set; } = new();
}

var admins = await session.Query<User>()
    .Where(x => x.Tags.Contains("admin"))
    .ToListAsync();
```

## Nested Property Access

Query nested objects directly:

```cs
public class Order
{
    public Guid Id { get; set; }
    public Address ShippingAddress { get; set; } = new();
}

var nyOrders = await session.Query<Order>()
    .Where(x => x.ShippingAddress.City == "New York")
    .ToListAsync();
```
