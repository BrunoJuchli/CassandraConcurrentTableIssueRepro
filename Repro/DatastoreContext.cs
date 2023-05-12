using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Repro
{
    public class DatastoreContext :
        IAsyncLifetime
    {
        public const string CollectionName = "DatastoreContext";

        private const string CassandraImage = "cassandra:3.11";
        private static readonly PortBinding CassandraPortBinding = new PortBinding(9043, 9042);
        private readonly CassandraTestcontainer _cassandra;

        public DatastoreContext()
        {
            // Cassandra
            _cassandra = CreateTestcontainer<CassandraTestcontainer>(
                CassandraImage,
                "cassandra-integration-testing",
                new[]
                {
                    CassandraPortBinding
                });
        }

        public ISession? CassandraSession { get; private set; }

        public string Keyspace => "testkeyspace";

        public async Task InitializeAsync()
        {
            await _cassandra.StartAsync();
            
            var session = await Cluster.Builder()
                .WithPort(CassandraPortBinding.HostPort)
                .WithCompression(CompressionType.LZ4)
                .AddContactPoints(new[] { "localhost"} )
                .WithExecutionProfiles(
                    profiles => profiles
                        .WithProfile(
                            "default",
                            profile => profile
                                .WithConsistencyLevel(ConsistencyLevel.One)))
                .Build()
                .ConnectAsync();

            CassandraSession = session;

            session.CreateKeyspaceIfNotExists(
                Keyspace,
                new Dictionary<string, string>
                {
                    { "class", "SimpleStrategy" },
                    { "replication_factor", "1" },
                });

            session.ChangeKeyspace(Keyspace);
            
            MappingConfiguration.Global.Define(ExportFormatCassandraEntity.Mapping);
            
            await session
                .GetTable<ExportFormatCassandraEntity>()
                .CreateIfNotExistsAsync();
        }

        public async Task DisposeAsync()
        {
            CassandraSession?.Dispose();
            await _cassandra.DisposeAsync();
        }

        private T CreateTestcontainer<T>(
            string image,
            string name,
            IReadOnlyList<PortBinding> portBindings)
            where T : TestcontainerDatabase
        {
            var builder = new TestcontainersBuilder<T>()
                .WithImage(image)
                .WithName(name);

            var waitStrategy = Wait.ForUnixContainer();

            foreach (var portBinding in portBindings)
            {
                builder = builder
                    .WithExposedPort(portBinding.ContainerPort)
                    .WithPortBinding(portBinding.HostPort, portBinding.ContainerPort);

                waitStrategy = waitStrategy.UntilPortIsAvailable(portBinding.ContainerPort);
            }

            builder = builder.WithWaitStrategy(waitStrategy);

            return builder.Build();
        }
    }
}
