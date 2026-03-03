using System.Data.Common;
using Polecat.Metadata;
using Polecat.Serialization;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

internal class LoadBatchQueryItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<T?> _tcs = new();
    private readonly object _id;
    private readonly DocumentProvider _provider;
    private readonly ISerializer _serializer;
    private readonly string _tenantId;

    public LoadBatchQueryItem(object id, DocumentProvider provider, ISerializer serializer, string tenantId)
    {
        _id = id;
        _provider = provider;
        _serializer = serializer;
        _tenantId = tenantId;
    }

    public Task<T?> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
    {
        var softDeleteFilter = _provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        builder.Append($"{_provider.SelectSql} WHERE id = ");
        builder.AppendParameter(_id);
        builder.Append(" AND tenant_id = ");
        builder.AppendParameter(_tenantId);
        builder.Append(softDeleteFilter);
        builder.Append(";\n");
    }

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        if (await reader.ReadAsync(token))
        {
            var json = reader.GetString(1); // data column
            var doc = _serializer.FromJson<T>(json);
            QuerySession.SyncVersionProperties(doc, reader, _provider);
            _tcs.SetResult(doc);
        }
        else
        {
            _tcs.SetResult(null);
        }
    }
}
