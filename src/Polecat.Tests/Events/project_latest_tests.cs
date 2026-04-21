using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record ReportCreated(string Title);
public record SectionAdded(string SectionName);
public record ReportPublished;

public class Report
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
    public int SectionCount { get; set; }
    public bool IsPublished { get; set; }

    public static Report Create(ReportCreated e) => new Report { Title = e.Title };
    public void Apply(SectionAdded e) => SectionCount++;
    public void Apply(ReportPublished e) => IsPublished = true;
}

[Collection("integration")]
public class project_latest_tests : IntegrationContext
{
    public project_latest_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task includes_pending_events_from_start_stream()
    {
        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();

        session.Events.StartStream(streamId,
            new ReportCreated("Q1 Report"),
            new SectionAdded("Revenue"),
            new SectionAdded("Costs")
        );

        var report = await session.Events.ProjectLatest<Report>(streamId);

        report.ShouldNotBeNull();
        report.Title.ShouldBe("Q1 Report");
        report.SectionCount.ShouldBe(2);
    }

    [Fact]
    public async Task includes_pending_events_after_committed_events()
    {
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId,
                new ReportCreated("Q1 Report"),
                new SectionAdded("Revenue")
            );
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(streamId,
                new SectionAdded("Costs"),
                new SectionAdded("Outlook"),
                new ReportPublished()
            );

            var report = await session.Events.ProjectLatest<Report>(streamId);

            report.ShouldNotBeNull();
            report.Title.ShouldBe("Q1 Report");
            report.SectionCount.ShouldBe(3);
            report.IsPublished.ShouldBeTrue();
        }
    }

    [Fact]
    public async Task no_pending_events_behaves_like_fetch_latest()
    {
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId,
                new ReportCreated("Q1 Report"),
                new SectionAdded("Revenue")
            );
            await session.SaveChangesAsync();
        }

        await using (var session = theStore.LightweightSession())
        {
            var fromProjectLatest = await session.Events.ProjectLatest<Report>(streamId);
            var fromFetchLatest = await session.Events.FetchLatest<Report>(streamId);

            fromProjectLatest.ShouldNotBeNull();
            fromFetchLatest.ShouldNotBeNull();
            fromProjectLatest.Title.ShouldBe(fromFetchLatest.Title);
            fromProjectLatest.SectionCount.ShouldBe(fromFetchLatest.SectionCount);
        }
    }

    [Fact]
    public async Task returns_null_for_nonexistent_stream()
    {
        await using var session = theStore.LightweightSession();
        var report = await session.Events.ProjectLatest<Report>(Guid.NewGuid());
        report.ShouldBeNull();
    }
}
