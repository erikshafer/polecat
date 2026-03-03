using System.Data.Common;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

/// <summary>
///     A single item in a batched query. Writes its SQL into a shared ICommandBuilder
///     and reads its result set when Execute() processes the reader.
/// </summary>
internal interface IBatchQueryItem
{
    void WriteSql(ICommandBuilder builder);
    Task ReadResultSetAsync(DbDataReader reader, CancellationToken token);
}
