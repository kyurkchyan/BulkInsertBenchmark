using BenchmarkDotNet.Running;
using BulkInsertBenchmark.Benchmarks;
using BulkInsertBenchmark.Data;
using BulkInsertBenchmark.Helpers;
using EFCore.BulkExtensions;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var connectionString = config["ConnectionString"]!;

// Ensure DB + schema exists
Console.WriteLine("Setting up database...");
using (var ctx = new AppDbContext(connectionString))
{
    ctx.Database.EnsureCreated();
    Console.WriteLine("Database ready.\n");
}

// --benchmark flag → hand off to BenchmarkDotNet (requires Release build)
if (args.Contains("--benchmark"))
{
    BenchmarkRunner.Run<InsertBenchmarks>();
    return;
}

// --- Quick comparison mode ---
// Usage: dotnet run -c Release [recordCount]   (default: 1_000_000)
int recordCount = 1_000_000;
foreach (var arg in args)
{
    if (int.TryParse(arg, out var n))
    {
        recordCount = n;
        break;
    }
}

Console.WriteLine($"Quick comparison — {recordCount:N0} records");
Console.WriteLine(new string('=', 55));

// Generate data once; both methods use the same dataset
Console.Write($"Generating {recordCount:N0} random orders... ");
var sw = Stopwatch.StartNew();
var orders = DataGenerator.Generate(recordCount);
sw.Stop();
Console.WriteLine($"done ({sw.Elapsed.TotalSeconds:F2}s)\n");

// ── 1. SQLBulkCopy ──────────────────────────────────────────
Console.WriteLine("[1/2] SQLBulkCopy");
SqlBulkCopyHelper.TruncateTable(connectionString);
sw.Restart();
SqlBulkCopyHelper.BulkInsert(connectionString, orders);
sw.Stop();
PrintResult("SqlBulkCopy", recordCount, sw.Elapsed);

// ── 2. EFCore.BulkExtensions ────────────────────────────────
Console.WriteLine("\n[2/2] EFCore.BulkExtensions");
SqlBulkCopyHelper.TruncateTable(connectionString);
sw.Restart();
using (var ctx = new AppDbContext(connectionString))
{
    var bulkConfig = new EFCore.BulkExtensions.BulkConfig
    {
        BatchSize = 10_000,
        SetOutputIdentity = false,
        PreserveInsertOrder = false
    };
    ctx.BulkInsert(orders, bulkConfig);
}
sw.Stop();
PrintResult("BulkExtensions", recordCount, sw.Elapsed);

// ── Cleanup ─────────────────────────────────────────────────
SqlBulkCopyHelper.TruncateTable(connectionString);

static void PrintResult(string label, int count, TimeSpan elapsed)
{
    double secs = elapsed.TotalSeconds;
    double rate = count / secs;
    Console.WriteLine($"  {label,-22} {secs,7:F2}s   {rate,14:N0} records/sec");
}
