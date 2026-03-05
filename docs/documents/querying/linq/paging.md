# Paging

Polecat provides built-in pagination support via the `IPagedList<T>` interface.

## ToPagedListAsync

The simplest way to paginate results:

```cs
var pagedList = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .ToPagedListAsync(pageNumber: 1, pageSize: 20);
```

::: tip
Page numbers are 1-based, not 0-based.
:::

## IPagedList Properties

The returned `IPagedList<T>` includes:

| Property | Description |
| :--- | :--- |
| `TotalItemCount` | Total items across all pages |
| `PageCount` | Total number of pages |
| `PageNumber` | Current page number (1-based) |
| `PageSize` | Items per page |
| `HasPreviousPage` | Whether a previous page exists |
| `HasNextPage` | Whether a next page exists |
| `IsFirstPage` | Whether this is the first page |
| `IsLastPage` | Whether this is the last page |
| `FirstItemOnPage` | 1-based index of first item on this page |
| `LastItemOnPage` | 1-based index of last item on this page |

## How It Works

`ToPagedListAsync` executes two queries:

1. A `COUNT(*)` query for the total number of matching documents
2. A `SELECT` with `OFFSET/FETCH` for the current page's data

Both queries run against the same filter criteria.

## Manual Paging

You can also page manually with `Skip` and `Take`:

```cs
var page2 = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .Skip(20)
    .Take(10)
    .ToListAsync();
```

::: warning
Always use `OrderBy` with paging to ensure consistent results across pages.
:::
