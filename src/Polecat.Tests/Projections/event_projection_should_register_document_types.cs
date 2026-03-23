using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Projections;

/// <summary>
/// Document type used in an EventProjection but NOT explicitly registered with Polecat.
/// The source generator should discover this type from Store/Insert calls in ApplyAsync
/// and register it automatically.
/// See https://github.com/JasperFx/marten/issues/4166
/// </summary>
public class AuditRecord
{
    public Guid Id { get; set; }
    public Guid StreamId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public DateTimeOffset Timestamp { get; set; }
}

public class AuditableEvent
{
    public string Description { get; set; } = string.Empty;
}

/// <summary>
/// An EventProjection with an explicit ApplyAsync override that stores a document type
/// using operations.Store&lt;T&gt;(). The source generator should detect this and emit a
/// constructor that registers AuditRecord as a published type.
/// See https://github.com/JasperFx/marten/issues/4166
/// </summary>
public partial class AuditRecordProjection : EventProjection
{
    public override ValueTask ApplyAsync(IDocumentSession operations, IEvent e, CancellationToken cancellation)
    {
        switch (e.Data)
        {
            case AuditableEvent:
                operations.Store<AuditRecord>(new AuditRecord
                {
                    Id = Guid.NewGuid(),
                    StreamId = e.StreamId,
                    EventType = e.Data.GetType().Name,
                    Timestamp = e.Timestamp
                });
                break;
        }

        return new ValueTask();
    }
}

/// <summary>
/// An EventProjection with conventional Create method that returns a document type.
/// The source generator should register this type via the emitted constructor.
/// </summary>
public partial class AuditRecordCreatorProjection : EventProjection
{
    public AuditRecord Create(AuditableEvent e) => new AuditRecord
    {
        Id = Guid.NewGuid(),
        EventType = nameof(AuditableEvent)
    };
}

public class event_projection_should_register_document_types
{
    [Fact]
    public void explicit_apply_async_projection_should_register_document_types()
    {
        // Issue #4166: Document types used in operations.Store<T>() inside an explicit
        // ApplyAsync override should be automatically discovered and registered
        // by the source generator.
        var projection = new AuditRecordProjection();

        var publishedTypes = projection.PublishedTypes().ToList();
        publishedTypes.Count.ShouldBeGreaterThan(0,
            "AuditRecordProjection should have published types");
        publishedTypes.ShouldContain(typeof(AuditRecord),
            "AuditRecord should be auto-discovered from operations.Store<AuditRecord>() in ApplyAsync");
    }

    [Fact]
    public void conventional_create_projection_should_register_document_types()
    {
        // EventProjection with Create method should also register the return type.
        var projection = new AuditRecordCreatorProjection();

        projection.PublishedTypes().ShouldContain(typeof(AuditRecord),
            "AuditRecord should be auto-discovered from Create method return type");
    }
}
