# Document Identity

Every document in Polecat must have a unique identity. Polecat supports several identity strategies.

## Supported ID Types

### Guid (Default)

```cs
public class User
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}
```

When `Id` is `Guid.Empty`, Polecat will automatically assign a new `Guid` on `Store()`.

### String

```cs
public class UserByEmail
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}
```

String IDs must be assigned by the application before storing.

### Int / Long with HiLo

```cs
public class Invoice
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
}
```

Numeric IDs are automatically assigned using the [HiLo algorithm](#hilo-sequences).

## Strongly Typed IDs

Polecat supports wrapper structs and records as document IDs:

```cs
public record struct UserId(Guid Value);

public class User
{
    public UserId Id { get; set; }
    public string Name { get; set; } = "";
}
```

Supported wrapper patterns:

- `record struct OrderId(Guid Value)` -- record struct with single property
- `readonly struct CustomerId { public int Value { get; init; } }` -- struct with init property
- Builder-pattern structs with a static `From` method

Polecat automatically detects wrapper types via JasperFx's `ValueTypeInfo` and handles wrapping/unwrapping for SQL operations, LINQ queries, identity maps, and bulk inserts.

::: tip
For strongly typed Guid IDs, Polecat will auto-assign the inner Guid value if it's empty, just like regular Guid IDs.
:::

## HiLo Sequences

For `int` and `long` ID types, Polecat uses the HiLo algorithm to generate unique IDs efficiently without round-tripping to the database for every insert.

### How It Works

1. The application reserves a block of IDs (the "Hi" value) from the `pc_hilo` table
2. IDs within the block are assigned sequentially in memory (the "Lo" values)
3. When the block is exhausted, a new "Hi" value is reserved

### Configuration

```cs
// Global defaults
opts.HiloSequenceDefaults.MaxLo = 500; // default is 1000

// Per-document type via attribute
[HiloSequence(MaxLo = 100)]
public class Invoice
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
}
```

### Resetting the Sequence Floor

```cs
await store.Advanced.ResetHiloSequenceFloor<Invoice>();
```

This scans existing documents and resets the HiLo sequence to start above the highest existing ID.

## ID Member Resolution

Polecat looks for an `Id` property on your document class. The property must be public with a getter and setter.
