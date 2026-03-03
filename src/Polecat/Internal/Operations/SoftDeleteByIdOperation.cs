using System.Data.Common;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class SoftDeleteByIdOperation : IStorageOperation
{
    private readonly object _id;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;

    public SoftDeleteByIdOperation(object id, DocumentMapping mapping, string tenantId)
    {
        _id = id;
        _mapping = mapping;
        _tenantId = tenantId;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Delete;
    public object? DocumentId => _id;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append(
            $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 1, deleted_at = SYSDATETIMEOFFSET() WHERE id = @id AND tenant_id = @tenant_id;");
        builder.AddParameters(new Dictionary<string, object?> { ["id"] = _id, ["tenant_id"] = _tenantId });
    }

    public Task PostprocessAsync(DbDataReader reader, CancellationToken token) => Task.CompletedTask;
}
