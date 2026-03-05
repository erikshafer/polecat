# Composite Projections

Composite projections orchestrate multiple projection stages that must run in a specific order. Within each stage, projections can run in parallel.

## How It Works

A composite projection defines stages:

1. **Stage 1**: Projections A and B run in parallel
2. **Stage 2**: Projection C runs after Stage 1 completes (it depends on A and B)
3. **Stage 3**: Projections D and E run in parallel after Stage 2

## Defining a Composite Projection

```cs
public class MyCompositeProjection : PolecatCompositeProjection
{
    public MyCompositeProjection()
    {
        // Stage 1: Run these in parallel
        AddProjection<OrderSummaryProjection>();
        AddProjection<InventoryProjection>();

        // Stage 2: Runs after stage 1
        AddProjection<DashboardProjection>(dependsOn: 1);
    }
}
```

## Registration

Composite projections are always async:

```cs
opts.Projections.Add<MyCompositeProjection>(ProjectionLifecycle.Async);
```

## Thread Safety

The `PolecatProjectionBatch` used by composite projections uses `ConcurrentBag` and `ConcurrentQueue` internally to safely handle parallel stage execution.

## Use Cases

- **Dependent read models** -- Dashboard that depends on individual aggregates
- **Multi-stage processing** -- Transform data through a pipeline
- **Performance optimization** -- Parallelize independent projections within a stage
