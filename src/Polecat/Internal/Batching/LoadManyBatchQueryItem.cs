using System.Data.Common;
using Polecat.Metadata;
using Polecat.Serialization;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

internal class LoadManyBatchQueryItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<IReadOnlyList<T>> _tcs = new();
    private readonly object[] _ids;
    private readonly DocumentProvider _provider;
    private readonly ISerializer _serializer;
    private readonly string _tenantId;

    public LoadManyBatchQueryItem(object[] ids, DocumentProvider provider, ISerializer serializer, string tenantId)
    {
        _ids = ids;
        _provider = provider;
        _serializer = serializer;
        _tenantId = tenantId;
    }

    public Task<IReadOnlyList<T>> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
    {
        if (_ids.Length == 0)
        {
            builder.Append($"{_provider.SelectSql} WHERE 1 = 0;\n");
            return;
        }

        var softDeleteFilter = _provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? " AND is_deleted = 0"
            : "";

        builder.Append($"{_provider.SelectSql} WHERE id IN (");
        for (var i = 0; i < _ids.Length; i++)
        {
            if (i > 0) builder.Append(", ");
            builder.AppendParameter(_ids[i]);
        }

        builder.Append(") AND tenant_id = ");
        builder.AppendParameter(_tenantId);
        builder.Append(softDeleteFilter);
        builder.Append(";\n");
    }

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        var results = new List<T>();
        while (await reader.ReadAsync(token))
        {
            var json = reader.GetString(1); // data column
            var doc = _serializer.FromJson<T>(json);
            QuerySession.SyncVersionProperties(doc, reader, _provider);
            results.Add(doc);
        }

        _tcs.SetResult(results);
    }
}
