using Cassandra;
using Cassandra.Data.Linq;

namespace Repro
{
    public class PersistencyExportFormatsCassandra
    {
        private readonly ISession session;

        public PersistencyExportFormatsCassandra(
            ISession session)
        {
            this.session = session;
        }

        public void AddOrUpdate(ExportFormatCassandraEntity model)
        {
            CreateAddOrUpdateStatement(model)
                .Execute();
        }

        public Task AddOrUpdateAsync(ExportFormatCassandraEntity model)
        {
            return CreateAddOrUpdateStatement(model)
                .ExecuteAsync();
        }

        private CqlInsert<ExportFormatCassandraEntity> CreateAddOrUpdateStatement(ExportFormatCassandraEntity table)
        {
            return session.GetTable<ExportFormatCassandraEntity>()
                .Insert(table);
        }

        public void Delete(ExportFormatCassandraEntity entity)
        {
            session.GetTable<ExportFormatCassandraEntity>()
                .Where(x => x.ServerUserID == entity.ServerUserID && x.ID == entity.ID)
                .Delete()
                .Execute();
        }

        public IEnumerable<ExportFormatCassandraEntity> GetAll()
        {
            return session
                .GetTable<ExportFormatCassandraEntity>()
                .Execute();
        }

        public IEnumerable<ExportFormatCassandraEntity> GetAllForServerUser(long serverUserId)
        {
            return session.GetTable<ExportFormatCassandraEntity>()
                .Where(x => x.ServerUserID == serverUserId)
                .Execute();
        }

        public ExportFormatCassandraEntity? Get(long serverUserId, Guid id)
        {
            return session
                .GetTable<ExportFormatCassandraEntity>()
                .Where(x => x.ServerUserID == serverUserId && x.ID == id)
                .Execute()
                .FirstOrDefault();
        }
    }
}
