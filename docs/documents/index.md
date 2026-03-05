# Polecat as Document DB

Polecat allows you to use SQL Server 2025 as a document database. Documents are stored as JSON using the native `json` data type, with each document type getting its own table (prefixed with `pc_doc_`).

## Key Concepts

- **Documents** are plain .NET objects serialized to JSON
- **Sessions** provide a unit of work pattern for batching changes
- **LINQ queries** translate to SQL Server JSON path expressions
- **Automatic schema management** creates and updates tables as needed

## Document Tables

Each document type gets its own table:

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | Varies | Primary key (Guid, string, int, long) |
| `data` | `json` | Serialized document body |
| `type` | `nvarchar(250)` | .NET type discriminator |
| `last_modified` | `datetimeoffset` | Last modification timestamp |
| `created` | `datetimeoffset` | Creation timestamp |
| `dotnet_type` | `nvarchar(500)` | Full .NET type name |
| `tenant_id` | `nvarchar(250)` | Tenant identifier (conjoined tenancy) |

Additional columns are added for features like [soft deletes](/documents/deletes#soft-deletes), [versioning](/documents/concurrency), and [metadata](/documents/metadata).

## Quick Example

```cs
// Define a document
public class User
{
    public Guid Id { get; set; }
    public string FirstName { get; set; } = "";
    public string LastName { get; set; } = "";
    public string Email { get; set; } = "";
}

// Store a document
await using var session = store.LightweightSession();
var user = new User { FirstName = "Jane", LastName = "Doe", Email = "jane@example.com" };
session.Store(user);
await session.SaveChangesAsync();

// Load by ID
var loaded = await session.LoadAsync<User>(user.Id);

// Query with LINQ
var users = await session.Query<User>()
    .Where(x => x.LastName == "Doe")
    .ToListAsync();
```
