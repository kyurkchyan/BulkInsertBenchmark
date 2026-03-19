using BenchmarkDotNet.Running;
using BulkInsertBenchmark.Benchmarks;
using BulkInsertBenchmark.Data;
using BulkInsertBenchmark.Helpers;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
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

    var mode = args.FirstOrDefault() ?? "insert";

    switch (mode)
    {
        case "--benchmark":
            BenchmarkRunner.Run<InsertBenchmarks>();
            BenchmarkRunner.Run<UpdateBenchmarks>();
            BenchmarkRunner.Run<DeleteBenchmarks>();
            break;

        case "update":
            await RunUpdateComparison(connectionString, ParseCount(args, 100_000));
            break;

        case "delete":
            await RunDeleteComparison(connectionString, ParseCount(args, 100_000));
            break;

        default: // insert (or a plain number)
            await RunInsertComparison(connectionString, ParseCount(args, 1_000_000));
            break;
    }
}
finally
{
    await ContainerFixture.StopAsync();
}

// ── Insert ───────────────────────────────────────────────────────────────────

static async Task RunInsertComparison(string cs, int count)
{
    Console.WriteLine($"INSERT comparison — {count:N0} records");
    PrintSeparator();

    Console.Write($"Generating {count:N0} random orders... ");
    var sw = Stopwatch.StartNew();
    var orders = DataGenerator.Generate(count);
    sw.Stop();
    Console.WriteLine($"done ({sw.Elapsed.TotalSeconds:F2}s)\n");

    Console.WriteLine("[1/2] SqlBulkCopy");
    SqlBulkCopyHelper.TruncateTable(cs);
    sw.Restart();
    SqlBulkCopyHelper.BulkInsert(cs, orders);
    sw.Stop();
    PrintResult("SqlBulkCopy", count, sw.Elapsed);

    Console.WriteLine("\n[2/2] EFCore.BulkExtensions");
    SqlBulkCopyHelper.TruncateTable(cs);
    sw.Restart();
    using (var ctx = new AppDbContext(cs))
    {
        ctx.BulkInsert(orders, new BulkConfig
        {
            BatchSize = 10_000,
            SetOutputIdentity = false,
            PreserveInsertOrder = false
        });
    }
    sw.Stop();
    PrintResult("BulkExtensions", count, sw.Elapsed);

    SqlBulkCopyHelper.TruncateTable(cs);
    await Task.CompletedTask;
}

// ── Update ───────────────────────────────────────────────────────────────────

static async Task RunUpdateComparison(string cs, int count)
{
    Console.WriteLine($"UPDATE comparison — {count:N0} records");
    PrintSeparator();

    Console.Write($"Seeding {count:N0} rows... ");
    SqlBulkCopyHelper.TruncateTable(cs);
    SqlBulkCopyHelper.BulkInsert(cs, DataGenerator.Generate(count));
    Console.WriteLine("done\n");

    // Loop + SaveChanges
    Console.WriteLine("[1/2] Loop + SaveChanges");
    var sw = Stopwatch.StartNew();
    using (var ctx = new AppDbContext(cs))
    {
        var orders = await ctx.Orders.ToListAsync();
        foreach (var o in orders)
            o.Status = "Completed";
        await ctx.SaveChangesAsync();
    }
    sw.Stop();
    PrintResult("Loop + SaveChanges", count, sw.Elapsed);

    // Re-seed
    SqlBulkCopyHelper.TruncateTable(cs);
    SqlBulkCopyHelper.BulkInsert(cs, DataGenerator.Generate(count));

    // ExecuteUpdateAsync
    Console.WriteLine("\n[2/2] ExecuteUpdateAsync");
    sw.Restart();
    using (var ctx = new AppDbContext(cs))
    {
        await ctx.Orders.ExecuteUpdateAsync(s =>
            s.SetProperty(o => o.Status, "Completed"));
    }
    sw.Stop();
    PrintResult("ExecuteUpdateAsync", count, sw.Elapsed);

    SqlBulkCopyHelper.TruncateTable(cs);
}

// ── Delete ───────────────────────────────────────────────────────────────────

static async Task RunDeleteComparison(string cs, int count)
{
    Console.WriteLine($"DELETE comparison — {count:N0} records");
    PrintSeparator();

    // Loop + SaveChanges
    Console.Write($"Seeding {count:N0} rows... ");
    SqlBulkCopyHelper.TruncateTable(cs);
    SqlBulkCopyHelper.BulkInsert(cs, DataGenerator.Generate(count));
    Console.WriteLine("done\n");

    Console.WriteLine("[1/2] Loop + SaveChanges");
    var sw = Stopwatch.StartNew();
    using (var ctx = new AppDbContext(cs))
    {
        var orders = await ctx.Orders.ToListAsync();
        ctx.Orders.RemoveRange(orders);
        await ctx.SaveChangesAsync();
    }
    sw.Stop();
    PrintResult("Loop + SaveChanges", count, sw.Elapsed);

    // Re-seed
    Console.Write($"\nRe-seeding {count:N0} rows... ");
    SqlBulkCopyHelper.BulkInsert(cs, DataGenerator.Generate(count));
    Console.WriteLine("done\n");

    // ExecuteDeleteAsync
    Console.WriteLine("[2/2] ExecuteDeleteAsync");
    sw.Restart();
    using (var ctx = new AppDbContext(cs))
    {
        await ctx.Orders.ExecuteDeleteAsync();
    }
    sw.Stop();
    PrintResult("ExecuteDeleteAsync", count, sw.Elapsed);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

static int ParseCount(string[] args, int defaultValue)
{
    foreach (var a in args)
        if (int.TryParse(a, out var n)) return n;
    return defaultValue;
}

static void PrintSeparator() => Console.WriteLine(new string('─', 58));

static void PrintResult(string label, int count, TimeSpan elapsed)
{
    double secs = elapsed.TotalSeconds;
    double rate = count / secs;
    Console.WriteLine($"  {label,-24} {secs,7:F2}s   {rate,14:N0} records/sec");
}
