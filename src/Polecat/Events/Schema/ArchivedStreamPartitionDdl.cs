using System.Data.Common;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Events.Schema;

/// <summary>
///     Generates SQL Server partition function and partition scheme DDL
///     for partitioning pc_events by is_archived. This must be created
///     before the events table so the CREATE TABLE ... ON clause works.
/// </summary>
internal class ArchivedStreamPartitionDdl : ISchemaObject
{
    private readonly EventGraph _events;

    public ArchivedStreamPartitionDdl(EventGraph events)
    {
        _events = events;
    }

    public string PartitionFunctionName => "pf_pc_events_is_archived";
    public string PartitionSchemeName => "ps_pc_events_is_archived";

    public DbObjectName Identifier => new SqlServerObjectName(_events.DatabaseSchemaName, PartitionFunctionName);

    public void WriteCreateStatement(Migrator rules, TextWriter writer)
    {
        // Drop existing (idempotent)
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{PartitionSchemeName}')");
        writer.WriteLine($"    DROP PARTITION SCHEME [{PartitionSchemeName}];");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{PartitionFunctionName}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION [{PartitionFunctionName}];");

        // Create partition function: splits on is_archived=1 (RANGE RIGHT)
        // Partition 1: is_archived < 1 (active events, is_archived=0)
        // Partition 2: is_archived >= 1 (archived events, is_archived=1)
        writer.WriteLine($"CREATE PARTITION FUNCTION [{PartitionFunctionName}] (bit) AS RANGE RIGHT FOR VALUES (1);");

        // Create partition scheme: all partitions on PRIMARY filegroup
        writer.WriteLine($"CREATE PARTITION SCHEME [{PartitionSchemeName}] AS PARTITION [{PartitionFunctionName}] ALL TO ([PRIMARY]);");
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{PartitionSchemeName}')");
        writer.WriteLine($"    DROP PARTITION SCHEME [{PartitionSchemeName}];");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{PartitionFunctionName}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION [{PartitionFunctionName}];");
    }

    public void ConfigureQueryCommand(Weasel.Core.DbCommandBuilder builder)
    {
        // Check if partition function exists
        builder.Append($"SELECT COUNT(*) FROM sys.partition_functions WHERE name = '{PartitionFunctionName}';");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken token)
    {
        await reader.ReadAsync(token);
        var count = await reader.GetFieldValueAsync<int>(0, token);
        var diff = count > 0 ? SchemaPatchDifference.None : SchemaPatchDifference.Create;
        return new SchemaObjectDelta(this, diff);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }
}
