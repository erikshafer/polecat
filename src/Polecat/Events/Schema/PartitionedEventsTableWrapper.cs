using System.Data.Common;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Polecat.Events.Schema;

/// <summary>
///     Wraps the EventsTable to inject SQL Server partition scheme ON clause
///     into the CREATE TABLE DDL. The normal Weasel Table.WriteCreateStatement
///     writes "); " but SQL Server needs ") ON [ps_name]([column]);"
/// </summary>
internal class PartitionedEventsTableWrapper : ISchemaObject
{
    private readonly EventsTable _inner;

    public PartitionedEventsTableWrapper(EventsTable inner)
    {
        _inner = inner;
    }

    public DbObjectName Identifier => _inner.Identifier;

    public void WriteCreateStatement(Migrator rules, TextWriter writer)
    {
        // Generate the normal DDL into a string
        var innerWriter = new StringWriter();
        _inner.WriteCreateStatement(rules, innerWriter);
        var ddl = innerWriter.ToString();

        // Replace the closing ");" with ") ON [ps_pc_events_is_archived]([is_archived]);"
        // The Table DDL ends with ");\r\n" or ");\n" at the end of the CREATE TABLE block.
        // We need to find the FIRST ");" which closes the column list.
        var onClause = " ON [ps_pc_events_is_archived]([is_archived])";
        var idx = ddl.IndexOf(");", StringComparison.Ordinal);
        if (idx >= 0)
        {
            ddl = ddl[..idx] + ")" + onClause + ";" + ddl[(idx + 2)..];
        }

        writer.Write(ddl);
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        _inner.WriteDropStatement(rules, writer);
    }

    public void ConfigureQueryCommand(Weasel.Core.DbCommandBuilder builder)
    {
        _inner.ConfigureQueryCommand(builder);
    }

    public Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken token)
    {
        return _inner.CreateDeltaAsync(reader, token);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        return _inner.AllNames();
    }
}
