using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Represents a fragment of SQL that can be applied to an ICommandBuilder.
/// </summary>
internal interface ISqlFragment
{
    void Apply(ICommandBuilder builder);
}
