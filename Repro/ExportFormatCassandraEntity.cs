using Cassandra.Mapping;

namespace Repro;

public record ExportFormatCassandraEntity
{
    public required long ServerUserID { get; init; }
    public required Guid ID { get; init; }
    public required string Name { get; init; }
    public required int ExportFormatType { get; init; }
    public required byte[] Configuration { get; init; }

    public static readonly Map<ExportFormatCassandraEntity> Mapping =
        new Map<ExportFormatCassandraEntity>()
            .CaseSensitive()
            .TableName("ExportFormat")
            .ExplicitColumns()
            .Column(t => t.ServerUserID, col => col.WithName("ServerUserID"))
            .Column(t => t.ID, col => col.WithName("ID"))
            .Column(t => t.Name, col => col.WithName("Name"))
            .Column(t => t.ExportFormatType, col => col.WithName("ExportFormatType"))
            .Column(t => t.Configuration, col => col.WithName("Configuration"))
            .PartitionKey("ServerUserID")
            .ClusteringKey("ID");
}