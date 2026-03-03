namespace Polecat.Logging;

/// <summary>
///     Store-level logger factory. Creates session loggers for each new session.
/// </summary>
public interface IPolecatLogger
{
    IPolecatSessionLogger StartSession(IQuerySession session);
}

/// <summary>
///     Per-session logger that receives SQL command lifecycle events.
/// </summary>
public interface IPolecatSessionLogger
{
    void OnBeforeExecute(string commandText);
    void LogSuccess(string commandText);
    void LogFailure(string commandText, Exception ex);
    void RecordSavedChanges(IDocumentSession session);
}

/// <summary>
///     Default no-op logger implementations.
/// </summary>
public class NullPolecatLogger : IPolecatLogger, IPolecatSessionLogger
{
    public static readonly NullPolecatLogger Instance = new();

    public IPolecatSessionLogger StartSession(IQuerySession session) => this;
    public void OnBeforeExecute(string commandText) { }
    public void LogSuccess(string commandText) { }
    public void LogFailure(string commandText, Exception ex) { }
    public void RecordSavedChanges(IDocumentSession session) { }
}
