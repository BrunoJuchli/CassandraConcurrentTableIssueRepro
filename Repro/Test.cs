using System.Collections.Immutable;
using Bogus;
using FluentAssertions;
using Xunit.Abstractions;

namespace Repro;

[Collection(DatastoreContext.CollectionName)]
public class Test
{
    private readonly Faker<ExportFormatCassandraEntity> entityFaker = new Faker<ExportFormatCassandraEntity>()
        .CustomInstantiator(f => 
            new ExportFormatCassandraEntity
            {
                ID = Guid.NewGuid(),
                ServerUserID = f.Random.Int(-5000, 5000),
                Configuration = new byte[] { 0x55, 0x33, 0x23 },
                Name = f.Lorem.Word(),
                ExportFormatType = f.Random.Int(0, 10)
            });

    private readonly InterlockedExchangeable<ImmutableList<ExportFormatCassandraEntity>> addedEntities =
        new(ImmutableList<ExportFormatCassandraEntity>.Empty);

    private readonly ITestOutputHelper testOutput;

    private readonly PersistencyExportFormatsCassandra persistency;

    public Test(
        DatastoreContext context,
        ITestOutputHelper testOutput)
    {
        this.testOutput = testOutput;
        this.persistency = new PersistencyExportFormatsCassandra(
            context.CassandraSession ?? throw new Exception("not initialized"));
    }

    [Fact]
    public async Task Test1()
    {
        var initialEntities = 
            entityFaker
                .GenerateForever()
                .Take(500)
                .ToImmutableList();

        await Task.WhenAll(
            initialEntities
                .Select(persistency.AddOrUpdateAsync)
                .ToArray());
        
        addedEntities.UpdateUnsafe(initialEntities);
        
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(90));

        var entityAddingTasks = Enumerable
            .Range(1, 10)
            .Select(_ =>
                AddEntitiesAsync(
                    TimeSpan.FromMilliseconds(10),
                    cts.Token))
            .ToArray();
        
        var queryAllTasks = Enumerable
            .Range(1, 3)
            .Select(_ =>
                Task.Run(() => LoopQueryAll(cts.Token)))
            .ToArray();

        var allTasks = entityAddingTasks
            .Concat(queryAllTasks)
            .ToArray();

        await Task.WhenAny(allTasks);
        
        cts.Cancel();
        
        await Task.WhenAll(allTasks);
        
        testOutput.WriteLine(
            $"ended without error. Number of added entities: {addedEntities.Value.Count}");
    }

    private async Task AddEntitiesAsync(
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(
                    delay,
                    cancellationToken);
                var entity = entityFaker.Generate();
                await persistency.AddOrUpdateAsync(entity);
                addedEntities.UpdateOrThrow(original => original.Add(entity));
            }
        }
        catch (TaskCanceledException)
        {
        }

    }

    private void LoopQueryAll(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            QueryAll();
        }
    }

    private void QueryAll()
    {
        var currentEntities = addedEntities.Value;
        var byUserId = currentEntities
            .GroupBy(x => x.ServerUserID)
            .ToArray();

        var expected = byUserId[
            Random.Shared.Next(
                0,
                byUserId.Length - 1)];

        var actual = persistency.GetAllForServerUser(expected.Key).ToArray();

        actual.Should()
            .HaveCountGreaterOrEqualTo(expected.Count());

        var all = persistency.GetAll().ToArray();

        all.Should().HaveCountGreaterOrEqualTo(currentEntities.Count);
    }
    
    private void LoopQuerySingle(
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            QuerySingle(
                cancellationToken);
        }
    }

    private void QuerySingle(
        CancellationToken cancellationToken)
    {
        var currenEntities = addedEntities.Value;
        for (
            int i = 0;
            i < 50 && !cancellationToken.IsCancellationRequested; 
            i++)
        {
            var expectedEntity = currenEntities[Random.Shared.Next(0, currenEntities.Count - 1)];

            var actualEntity = persistency.Get(
                expectedEntity.ServerUserID,
                expectedEntity.ID);

            actualEntity.Should()
                .Be(expectedEntity);
        }
    }
}