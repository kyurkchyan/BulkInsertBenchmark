using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using BulkInsertBenchmark.Data;
using BulkInsertBenchmark.Helpers;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;

namespace BulkInsertBenchmark.Benchmarks;

// ── UPDATE ──────────────────────────────────────────────────────────────────

[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class UpdateBenchmarks
{
    private string _connectionString = string.Empty;

    [Params(10_000, 50_000)]
    public int RecordCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _connectionString = ContainerFixture.ConnectionString;
        using var ctx = new AppDbContext(_connectionString);
        ctx.Database.EnsureCreated();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        SqlBulkCopyHelper.TruncateTable(_connectionString);
        SqlBulkCopyHelper.BulkInsert(_connectionString, DataGenerator.Generate(RecordCount));
    }

    /// <summary>Load all rows into memory, mutate each one, SaveChanges (N round-trips).</summary>
    [Benchmark(Baseline = true, Description = "Loop + SaveChanges")]
    public async Task LoopUpdate()
    {
        using var ctx = new AppDbContext(_connectionString);
        var orders = await ctx.Orders.ToListAsync();
        foreach (var o in orders)
            o.Status = "Completed";
        await ctx.SaveChangesAsync();
    }

    /// <summary>Single SQL UPDATE via ExecuteUpdateAsync — no entity loading.</summary>
    [Benchmark(Description = "ExecuteUpdateAsync")]
    public async Task ExecuteUpdate()
    {
        using var ctx = new AppDbContext(_connectionString);
        await ctx.Orders.ExecuteUpdateAsync(s =>
            s.SetProperty(o => o.Status, "Completed"));
    }

    [GlobalCleanup]
    public void GlobalCleanup() => SqlBulkCopyHelper.TruncateTable(_connectionString);
}

// ── DELETE ──────────────────────────────────────────────────────────────────

[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class DeleteBenchmarks
{
    private string _connectionString = string.Empty;

    [Params(10_000, 50_000)]
    public int RecordCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _connectionString = ContainerFixture.ConnectionString;
        using var ctx = new AppDbContext(_connectionString);
        ctx.Database.EnsureCreated();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        SqlBulkCopyHelper.TruncateTable(_connectionString);
        SqlBulkCopyHelper.BulkInsert(_connectionString, DataGenerator.Generate(RecordCount));
    }

    /// <summary>Load all rows, RemoveRange, SaveChanges.</summary>
    [Benchmark(Baseline = true, Description = "Loop + SaveChanges")]
    public async Task LoopDelete()
    {
        using var ctx = new AppDbContext(_connectionString);
        var orders = await ctx.Orders.ToListAsync();
        ctx.Orders.RemoveRange(orders);
        await ctx.SaveChangesAsync();
    }

    /// <summary>Single SQL DELETE via ExecuteDeleteAsync — no entity loading.</summary>
    [Benchmark(Description = "ExecuteDeleteAsync")]
    public async Task ExecuteDelete()
    {
        using var ctx = new AppDbContext(_connectionString);
        await ctx.Orders.ExecuteDeleteAsync();
    }

    [GlobalCleanup]
    public void GlobalCleanup() => SqlBulkCopyHelper.TruncateTable(_connectionString);
}
