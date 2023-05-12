using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Logging;

namespace Repro
{
    public class CassandraTestcontainer :
        TestcontainerDatabase
    {
        internal CassandraTestcontainer(
            ITestcontainersConfiguration configuration,
            ILogger logger)
          : base(
              configuration,
              logger)
        {
        }

        public override string ConnectionString
          => $"{Hostname}:{Port}";
    }
}
