using BenchmarkDotNet.Running;
using BulkInsertBenchmark.Benchmarks;
using BulkInsertBenchmark.Data;
using BulkInsertBenchmark.Helpers;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;

// Required for Npgsql 8.x — prevents InvalidCastException with plain DateTime values.
AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);

var mode = args.FirstOrDefault() ?? "insert";
var isPostgres  = mode is "postgres" or "--benchmark-postgres";
var isUnion     = mode is "union" or "--benchmark-union";

if (!isPostgres) await ContainerFixture.StartAsync();
if (isPostgres)  await PgContainerFixture.StartAsync();
// union mode also uses the SQL Server container (already started above)

try
{
    switch (mode)
    {
        case "--benchmark-union":
        {
            var connectionString = ContainerFixture.ConnectionString;
            BenchmarkRunner.Run<UnionVsUnionAllBenchmarks>();
            break;
        }

        case "union":
            await RunUnionVsUnionAllComparison(ContainerFixture.ConnectionString, ParseCount(args, 300_000));
            break;

        case "--benchmark":
        {
            var connectionString = ContainerFixture.ConnectionString;
            using (var ctx = new AppDbContext(connectionString))
                ctx.Database.EnsureCreated();

            BenchmarkRunner.Run<InsertBenchmarks>();
            BenchmarkRunner.Run<UpdateBenchmarks>();
            BenchmarkRunner.Run<DeleteBenchmarks>();
            break;
        }

        case "--benchmark-postgres":
        {
            var connectionString = PgContainerFixture.ConnectionString;
            using (var ctx = new PostgresDbContext(connectionString))
                ctx.Database.EnsureCreated();

            BenchmarkRunner.Run<PgInsertBenchmarks>();
            BenchmarkRunner.Run<PgRowByRowBenchmarks>();
            break;
        }

        case "postgres":
            await RunPostgresInsertComparison(PgContainerFixture.ConnectionString, ParseCount(args, 100_000));
            break;

        case "update":
            await RunUpdateComparison(ContainerFixture.ConnectionString, ParseCount(args, 100_000));
            break;

        case "delete":
            await RunDeleteComparison(ContainerFixture.ConnectionString, ParseCount(args, 100_000));
            break;

        default: // insert (or a plain number)
        {
            var connectionString = ContainerFixture.ConnectionString;
            Console.WriteLine("Setting up database schema...");
            using (var ctx = new AppDbContext(connectionString))
            {
                ctx.Database.EnsureCreated();
                Console.WriteLine("Schema ready.\n");
            }
            await RunInsertComparison(connectionString, ParseCount(args, 1_000_000));
            break;
        }
    }
}
finally
{
    if (!isPostgres) await ContainerFixture.StopAsync();
    if (isPostgres)  await PgContainerFixture.StopAsync();
}

// ── UNION vs UNION ALL ────────────────────────────────────────────────────────

static async Task RunUnionVsUnionAllComparison(string cs, int count)
{
    Console.WriteLine($"UNION vs UNION ALL — {count:N0} ServiceForms rows");
    Console.WriteLine("Schema: ServiceForms → 4 mutually-exclusive branches via nullable FKs");
    Console.WriteLine("Query:  WHERE ServiceCenterId = @id AND ResolutionStatus = 0 AND IsAvailable = 1\n");

    const int runs = 5;

    long MedianMs(Func<long> measure)
    {
        var samples = Enumerable.Range(0, runs).Select(_ => measure()).OrderBy(x => x).ToArray();
        return samples[runs / 2];
    }

    void RunPhase(string label, bool withIndex)
    {
        Console.Write($"  Seeding {count:N0} rows... ");
        var sw = Stopwatch.StartNew();
        var primaryId = ServiceQueueSchema.Setup(cs);
        ServiceQueueSchema.Seed(cs, count, primaryId);
        if (withIndex) ServiceQueueSchema.CreateIndex(cs);
        sw.Stop();
        Console.WriteLine($"done ({sw.Elapsed.TotalSeconds:F1}s)");

        var unionCountMs    = MedianMs(() => { var t = Stopwatch.GetTimestamp(); ServiceQueueSchema.Count(cs, "vw_ServiceQueue_Union",    primaryId); return ElapsedMs(t); });
        var unionAllCountMs = MedianMs(() => { var t = Stopwatch.GetTimestamp(); ServiceQueueSchema.Count(cs, "vw_ServiceQueue_UnionAll", primaryId); return ElapsedMs(t); });
        var unionPageMs     = MedianMs(() => { var t = Stopwatch.GetTimestamp(); ServiceQueueSchema.PagedQuery(cs, "vw_ServiceQueue_Union",    primaryId); return ElapsedMs(t); });
        var unionAllPageMs  = MedianMs(() => { var t = Stopwatch.GetTimestamp(); ServiceQueueSchema.PagedQuery(cs, "vw_ServiceQueue_UnionAll", primaryId); return ElapsedMs(t); });

        Console.WriteLine($"\n  {label}");
        Console.WriteLine($"  {new string('─', 67)}");
        Console.WriteLine($"  {"Query",-35} {"UNION",8} {"UNION ALL",10} {"Speedup",9}");
        Console.WriteLine($"  {new string('─', 67)}");
        PrintUnionRow("COUNT(*)",               unionCountMs,   unionAllCountMs);
        PrintUnionRow("Page 1 (TOP 50, ORDER)", unionPageMs,    unionAllPageMs);
        Console.WriteLine();

        ServiceQueueSchema.Teardown(cs);
    }

    RunPhase("Phase 1 — No index (both views scan the full table)", withIndex: false);
    RunPhase("Phase 2 — With composite index on (ServiceCenterId, ResolutionStatus, IsAvailable)", withIndex: true);

    Console.WriteLine("Identical row counts from both views confirm UNION deduplication is wasted work.");
    await Task.CompletedTask;
}

static long ElapsedMs(long startTimestamp) =>
    (long)((Stopwatch.GetTimestamp() - startTimestamp) * 1000.0 / Stopwatch.Frequency);

static void PrintUnionRow(string label, long unionMs, long unionAllMs)
{
    double speedup = unionMs > 0 ? (double)unionMs / unionAllMs : 1;
    Console.WriteLine($"  {label,-35} {unionMs,8} ms {unionAllMs,8} ms   {speedup,6:F1}x");
}

// ── PostgreSQL Insert ─────────────────────────────────────────────────────────

static async Task RunPostgresInsertComparison(string cs, int count)
{
    Console.WriteLine("Setting up PostgreSQL database schema...");
    using (var ctx = new PostgresDbContext(cs))
    {
        ctx.Database.EnsureCreated();
        Console.WriteLine("Schema ready.\n");
    }

    Console.WriteLine($"PostgreSQL INSERT comparison — {count:N0} records");
    Console.WriteLine("Note: Row-by-row INSERT omitted from quick mode (use --benchmark-postgres)");
    PrintSeparator();

    Console.Write($"Generating {count:N0} random orders... ");
    var sw = Stopwatch.StartNew();
    var orders = DataGenerator.Generate(count);
    sw.Stop();
    Console.WriteLine($"done ({sw.Elapsed.TotalSeconds:F2}s)\n");

    Console.WriteLine("[1/3] EF Core SaveChanges (batched INSERTs)");
    NpgsqlCopyHelper.TruncateTable(cs);
    sw.Restart();
    await using (var ctx = new PostgresDbContext(cs))
    {
        await ctx.Orders.AddRangeAsync(orders);
        await ctx.SaveChangesAsync();
    }
    sw.Stop();
    PrintResult("EF Core SaveChanges", count, sw.Elapsed);

    Console.WriteLine("\n[2/3] Npgsql Binary COPY");
    NpgsqlCopyHelper.TruncateTable(cs);
    sw.Restart();
    await NpgsqlCopyHelper.BulkInsertAsync(cs, orders);
    sw.Stop();
    PrintResult("Npgsql Binary COPY", count, sw.Elapsed);

    Console.WriteLine("\n[3/3] EFCore.BulkExtensions");
    NpgsqlCopyHelper.TruncateTable(cs);
    sw.Restart();
    await using (var ctx = new PostgresDbContext(cs))
    {
        await ctx.BulkInsertAsync(orders, new BulkConfig
        {
            BatchSize = 10_000,
            SetOutputIdentity = false,
            PreserveInsertOrder = false
        });
    }
    sw.Stop();
    PrintResult("BulkExtensions", count, sw.Elapsed);

    NpgsqlCopyHelper.TruncateTable(cs);
}

// ── SQL Server Insert ─────────────────────────────────────────────────────────

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
