using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BulkInsertBenchmark.Data;
using BulkInsertBenchmark.Helpers;
using BulkInsertBenchmark.Models;
using EFCore.BulkExtensions;
using Microsoft.Extensions.Configuration;

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
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false)
            .Build();

        _connectionString = config["ConnectionString"]!;

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
        var bulkConfig = new BulkConfig
        {
            BatchSize = 10_000,
            SetOutputIdentity = false,
            PreserveInsertOrder = false
        };
        ctx.BulkInsert(_orders, bulkConfig);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        SqlBulkCopyHelper.TruncateTable(_connectionString);
    }
}

public class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig()
    {
        AddJob(Job.Default
            .WithWarmupCount(1)
            .WithIterationCount(3)
            .WithInvocationCount(1)
            .WithUnrollFactor(1));
    }
}
