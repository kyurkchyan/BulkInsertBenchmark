using BenchmarkDotNet.Running;
using BulkInsertBenchmark.Benchmarks;
using BulkInsertBenchmark.Data;
using BulkInsertBenchmark.Helpers;
using EFCore.BulkExtensions;
using System.Diagnostics;

await ContainerFixture.StartAsync();

try
{
    var connectionString = ContainerFixture.ConnectionString;

    Console.WriteLine("Setting up database schema...");
    using (var ctx = new AppDbContext(connectionString))
    {
        ctx.Database.EnsureCreated();
        Console.WriteLine("Schema ready.\n");
    }

    // --benchmark → full BenchmarkDotNet run (in-process, container already running)
    if (args.Contains("--benchmark"))
    {
        BenchmarkRunner.Run<InsertBenchmarks>();
        return;
    }

    // Quick timed comparison (default)
    // Usage: dotnet run -c Release [recordCount]   (default: 1_000_000)
    int recordCount = 1_000_000;
    foreach (var arg in args)
    {
        if (int.TryParse(arg, out var n)) { recordCount = n; break; }
    }

    Console.WriteLine($"Quick comparison — {recordCount:N0} records");
    Console.WriteLine(new string('─', 58));

    Console.Write($"Generating {recordCount:N0} random orders... ");
    var sw = Stopwatch.StartNew();
    var orders = DataGenerator.Generate(recordCount);
    sw.Stop();
    Console.WriteLine($"done ({sw.Elapsed.TotalSeconds:F2}s)\n");

    // ── 1. SqlBulkCopy ──────────────────────────────────────
    Console.WriteLine("[1/2] SqlBulkCopy");
    SqlBulkCopyHelper.TruncateTable(connectionString);
    sw.Restart();
    SqlBulkCopyHelper.BulkInsert(connectionString, orders);
    sw.Stop();
    PrintResult("SqlBulkCopy", recordCount, sw.Elapsed);

    // ── 2. EFCore.BulkExtensions ────────────────────────────
    Console.WriteLine("\n[2/2] EFCore.BulkExtensions");
    SqlBulkCopyHelper.TruncateTable(connectionString);
    sw.Restart();
    using (var ctx = new AppDbContext(connectionString))
    {
        ctx.BulkInsert(orders, new BulkConfig
        {
            BatchSize = 10_000,
            SetOutputIdentity = false,
            PreserveInsertOrder = false
        });
    }
    sw.Stop();
    PrintResult("BulkExtensions", recordCount, sw.Elapsed);

    SqlBulkCopyHelper.TruncateTable(connectionString);
}
finally
{
    await ContainerFixture.StopAsync();
}

static void PrintResult(string label, int count, TimeSpan elapsed)
{
    double secs = elapsed.TotalSeconds;
    double rate = count / secs;
    Console.WriteLine($"  {label,-22} {secs,7:F2}s   {rate,14:N0} records/sec");
}
