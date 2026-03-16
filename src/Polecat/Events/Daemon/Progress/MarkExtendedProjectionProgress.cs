using System.Data.Common;
using JasperFx.Events.Projections;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Daemon.Progress;

/// <summary>
///     Upserts a projection progression row with extended agent state columns
///     (heartbeat, agent_status, pause_reason, running_on_node).
///     Used when EnableExtendedProgressionTracking is enabled.
/// </summary>
internal class MarkExtendedProjectionProgress : Polecat.Internal.IStorageOperation
{
    private readonly EventGraph _events;
    private readonly string _shardName;
    private readonly long _sequenceCeiling;
    private readonly DateTimeOffset _heartbeat;
    private readonly string? _agentStatus;
    private readonly string? _pauseReason;
    private readonly int? _runningOnNode;

    public MarkExtendedProjectionProgress(
        EventGraph events,
        string shardName,
        long sequenceCeiling,
        DateTimeOffset heartbeat,
        string? agentStatus,
        string? pauseReason,
        int? runningOnNode)
    {
        _events = events;
        _shardName = shardName;
        _sequenceCeiling = sequenceCeiling;
        _heartbeat = heartbeat;
        _agentStatus = agentStatus;
        _pauseReason = pauseReason;
        _runningOnNode = runningOnNode;
    }

    public Type DocumentType => typeof(ShardState);
    public OperationRole Role() => OperationRole.Upsert;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"""
            MERGE {_events.ProgressionTableName} AS target
            USING (SELECT @name AS name) AS source ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET
                last_seq_id = @seq,
                last_updated = SYSDATETIMEOFFSET(),
                heartbeat = @heartbeat,
                agent_status = @agent_status,
                pause_reason = @pause_reason,
                running_on_node = @running_on_node
            WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated, heartbeat, agent_status, pause_reason, running_on_node)
                VALUES (@name, @seq, SYSDATETIMEOFFSET(), @heartbeat, @agent_status, @pause_reason, @running_on_node);
            """);

        builder.AddParameters(new Dictionary<string, object?>
        {
            ["name"] = _shardName,
            ["seq"] = _sequenceCeiling,
            ["heartbeat"] = _heartbeat,
            ["agent_status"] = (object?)_agentStatus ?? DBNull.Value,
            ["pause_reason"] = (object?)_pauseReason ?? DBNull.Value,
            ["running_on_node"] = (object?)_runningOnNode ?? DBNull.Value
        });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
