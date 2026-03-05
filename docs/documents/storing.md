# Storing Documents

Polecat provides several methods for persisting documents to SQL Server.

## Store (Upsert)

The `Store()` method performs an upsert -- inserting new documents or updating existing ones:

```cs
await using var session = store.LightweightSession();

var user = new User { FirstName = "Alice", LastName = "Smith" };
session.Store(user);
// user.Id is now assigned (for Guid IDs)

await session.SaveChangesAsync();
```

Store multiple documents at once:

```cs
session.Store(user1, user2, user3);
```

## Insert

`Insert()` will throw if a document with the same ID already exists:

```cs
session.Insert(new User { FirstName = "Bob" });
await session.SaveChangesAsync();
```

## Update

`Update()` will throw if the document does not already exist:

```cs
var user = await session.LoadAsync<User>(id);
user.LastName = "Updated";
session.Update(user);
await session.SaveChangesAsync();
```

## SaveChangesAsync

All pending operations are flushed to the database in a single transaction:

```cs
session.Store(user1);
session.Insert(order1);
session.Delete<Invoice>(invoiceId);

// All three operations execute in one transaction
await session.SaveChangesAsync();
```

::: tip
`SaveChangesAsync()` processes event stream operations first, then document operations. This ensures inline projections can create documents from events in the same transaction.
:::

## ID Assignment

### Guid IDs

Automatically assigned on `Store()` or `Insert()` if the ID is `Guid.Empty`:

```cs
var user = new User(); // Id is Guid.Empty
session.Store(user);   // Id is now assigned
```

### Numeric IDs (int/long)

Automatically assigned via [HiLo sequences](/documents/identity#hilo-sequences):

```cs
var invoice = new Invoice(); // Id is 0
session.Store(invoice);      // Id is now assigned from HiLo
```

### String IDs

Must be assigned by the application before storing:

```cs
var doc = new MyDoc { Id = "custom-id-123" };
session.Store(doc);
```

### Strongly Typed IDs

Wrapper types are automatically handled:

```cs
public record struct OrderId(Guid Value);

var order = new Order(); // Id.Value is Guid.Empty
session.Store(order);    // Id.Value is now assigned
```
