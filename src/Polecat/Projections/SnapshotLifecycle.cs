using JasperFx.Events.Projections;

namespace Polecat.Projections;

/// <summary>
///     Lifecycle for snapshot (self-aggregating) projections registered via
///     <see cref="PolecatProjectionOptions.Snapshot{T}(SnapshotLifecycle)"/>.
/// </summary>
public enum SnapshotLifecycle
{
    /// <summary>
    ///     The snapshot will be updated in the same transaction as the events being captured.
    /// </summary>
    Inline,

    /// <summary>
    ///     The snapshot will be made asynchronously within the async projection daemon.
    /// </summary>
    Async
}

internal static class SnapshotLifecycleExtensions
{
    public static ProjectionLifecycle ToProjectionLifecycle(this SnapshotLifecycle lifecycle) =>
        lifecycle switch
        {
            SnapshotLifecycle.Inline => ProjectionLifecycle.Inline,
            SnapshotLifecycle.Async => ProjectionLifecycle.Async,
            _ => throw new ArgumentOutOfRangeException(nameof(lifecycle), lifecycle, null)
        };
}
