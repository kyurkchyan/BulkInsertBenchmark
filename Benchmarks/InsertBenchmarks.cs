using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using BulkInsertBenchmark.Data;
using BulkInsertBenchmark.Helpers;
using BulkInsertBenchmark.Models;
using EFCore.BulkExtensions;

namespace BulkInsertBenchmark.Benchmarks;

[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class InsertBenchmarks
{
    private string _connectionString = string.Empty;
    private List<Order> _orders = [];

    [Params(10_000, 100_000, 500_000)]
    public int RecordCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        // Container was started by Program.cs before BenchmarkRunner.Run was called.
        // InProcessEmitToolchain keeps us in the same process so the static field is shared.
        _connectionString = ContainerFixture.ConnectionString;

        using var ctx = new AppDbContext(_connectionString);
        ctx.Database.EnsureCreated();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _orders = DataGenerator.Generate(RecordCount);
        SqlBulkCopyHelper.TruncateTable(_connectionString);
    }

    [Benchmark(Baseline = true, Description = "SqlBulkCopy")]
    public void SqlBulkCopy()
    {
        SqlBulkCopyHelper.BulkInsert(_connectionString, _orders);
    }

    [Benchmark(Description = "EFCore.BulkExtensions")]
    public void EfCoreBulkExtensions()
    {
        using var ctx = new AppDbContext(_connectionString);
        ctx.BulkInsert(_orders, new BulkConfig
        {
            BatchSize = 10_000,
            SetOutputIdentity = false,
            PreserveInsertOrder = false
        });
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        SqlBulkCopyHelper.TruncateTable(_connectionString);
    }
}

/// <summary>
/// Uses InProcessEmitToolchain so benchmarks run in the same process as Program.cs,
/// allowing them to share the already-running Testcontainers SQL Server instance.
/// </summary>
public class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig()
    {
        AddJob(Job.Default
            .WithToolchain(InProcessEmitToolchain.Instance)
            .WithWarmupCount(1)
            .WithIterationCount(3)
            .WithInvocationCount(1)
            .WithUnrollFactor(1));
    }
}
