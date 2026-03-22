using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class archived_stream_partitioning : IntegrationContext
{
    public archived_stream_partitioning(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "archived_part";
            opts.EventGraph.UseArchivedStreamPartitioning = true;
        });
    }

    [Fact]
    public async Task events_table_is_partitioned()
    {
        // Write some events to ensure schema is created
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Partitioned Quest"),
            new MembersJoined(1, "Town", ["Alpha"]));
        await theSession.SaveChangesAsync();

        // Verify partition function exists
        var conn = await OpenConnectionAsync();

        await using var pfCmd = new SqlCommand(
            "SELECT COUNT(*) FROM sys.partition_functions WHERE name = 'pf_pc_events_is_archived'", conn);
        var pfCount = (int)(await pfCmd.ExecuteScalarAsync())!;
        pfCount.ShouldBeGreaterThan(0);

        // Verify partition scheme exists
        await using var psCmd = new SqlCommand(
            "SELECT COUNT(*) FROM sys.partition_schemes WHERE name = 'ps_pc_events_is_archived'", conn);
        var psCount = (int)(await psCmd.ExecuteScalarAsync())!;
        psCount.ShouldBeGreaterThan(0);

        // Verify the table has 2 partitions (is_archived=0 and is_archived=1)
        await using var partCmd = new SqlCommand("""
            SELECT COUNT(*)
            FROM sys.partitions p
            JOIN sys.objects o ON p.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = 'archived_part' AND o.name = 'pc_events' AND p.index_id <= 1
            """, conn);
        var partitionCount = (int)(await partCmd.ExecuteScalarAsync())!;
        partitionCount.ShouldBe(2);
    }

    [Fact]
    public async Task can_write_and_read_events_with_partitioning()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Readable Quest"),
            new MembersJoined(1, "Town", ["Beta", "Gamma"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(2);
        events[0].Data.ShouldBeOfType<QuestStarted>();
        events[1].Data.ShouldBeOfType<MembersJoined>();
    }

    [Fact]
    public async Task can_archive_and_query_active_events_only()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archive Quest"),
            new MembersJoined(1, "Town", ["Delta"]));
        await theSession.SaveChangesAsync();

        // Archive the stream
        theSession.Events.ArchiveStream(streamId);
        await theSession.SaveChangesAsync();

        // FetchStream should return empty for archived streams (default filters archived)
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(0);

        // Aggregate should return null for archived stream
        var aggregate = await query.Events.FetchLatest<QuestAggregate>(streamId);
        aggregate.ShouldBeNull();
    }
}
