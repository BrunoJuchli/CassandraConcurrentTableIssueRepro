using Cassandra;
using Cassandra.Data.Linq;

namespace Repro
{
    public class PersistencyExportFormatsCassandra
    {
        private readonly Table<ExportFormatCassandraEntity> table;

        public PersistencyExportFormatsCassandra(
            ISession session)
        {
            table = session.GetTable<ExportFormatCassandraEntity>();
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

        private CqlInsert<ExportFormatCassandraEntity> CreateAddOrUpdateStatement(ExportFormatCassandraEntity entity)
        {
            return table               
                .Insert(entity);
        }

        public void Delete(ExportFormatCassandraEntity entity)
        {
            table
                .Where(x => x.ServerUserID == entity.ServerUserID && x.ID == entity.ID)
                .Delete()
                .Execute();
        }

        public IEnumerable<ExportFormatCassandraEntity> GetAll()
        {
            return table.Execute();
        }

        public IEnumerable<ExportFormatCassandraEntity> GetAllForServerUser(long serverUserId)
        {
            return table
                .Where(x => x.ServerUserID == serverUserId)
                .Execute();
        }

        public ExportFormatCassandraEntity? Get(long serverUserId, Guid id)
        {
            return table
                .Where(x => x.ServerUserID == serverUserId && x.ID == id)
                .Execute()
                .FirstOrDefault();
        }
    }
}
