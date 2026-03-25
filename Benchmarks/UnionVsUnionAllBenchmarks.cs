using BenchmarkDotNet.Attributes;
using BulkInsertBenchmark.Helpers;
using Microsoft.Data.SqlClient;
using System.Data;

namespace BulkInsertBenchmark.Benchmarks;

// ─── Benchmark ────────────────────────────────────────────────────────────────
//
// Demonstrates the performance cost of UNION vs UNION ALL in a multi-branch
// SQL view, using an e-commerce warranty repair queue as the domain.
//
// The schema mirrors a real production pattern: a central ServiceForms table
// links to four mutually-exclusive source tables via nullable FK columns
// (OnServiceRequest, OnCustomerReturn, OnDirectExchange, OnStoreReturn).
// Each row belongs to exactly one branch — UNION deduplication is unnecessary
// work, and becomes actively harmful at scale.
//
// Two views are created over identical data:
//   vw_ServiceQueue_Union    — uses UNION    (deduplicates across all branches)
//   vw_ServiceQueue_UnionAll — uses UNION ALL (no deduplication, semantically identical)
//
// WHY WITHINDEX IS THE CRITICAL PARAMETER
// ────────────────────────────────────────
// Without an index, both views perform full table scans. The extra deduplication
// sort from UNION adds some overhead but both are roughly equal — the scan
// dominates. This is the misleading case: it makes UNION look "fine".
//
// The real problem surfaces when you add a composite index on
// (ServiceCenterId, ResolutionStatus, IsAvailable). With UNION ALL, SQL Server
// can push the WHERE predicate into each branch and use an index seek — touching
// only the matching rows. With UNION, SQL Server must materialise all rows from
// all branches and sort/hash them to deduplicate before it can count or page —
// the index seek happens per branch, but the deduplication blows away any saving.
//
// For COUNT in particular, UNION ALL allows SQL Server to sum branch counts
// without materialising rows at all. UNION cannot — it must compare every row.
// This is what caused 5 GB memory grants and 20 s timeouts in production.

[Config(typeof(BenchmarkConfig))]
[MemoryDiagnoser]
public class UnionVsUnionAllBenchmarks
{
    private string _connectionString = string.Empty;
    private Guid _serviceCenterId;

    /// <summary>Total ServiceForms rows seeded across all four branches.</summary>
    [Params(50_000, 300_000)]
    public int RecordCount { get; set; }

    /// <summary>
    /// Without index: both views do full scans — UNION overhead is minor.
    /// With index: UNION ALL gets a fast index seek per branch; UNION still
    /// materialises everything for deduplication — the gap becomes dramatic.
    /// </summary>
    [Params(false, true)]
    public bool WithIndex { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _connectionString = ContainerFixture.ConnectionString;
        _serviceCenterId  = ServiceQueueSchema.Setup(_connectionString);
        ServiceQueueSchema.Seed(_connectionString, RecordCount, _serviceCenterId);
        if (WithIndex) ServiceQueueSchema.CreateIndex(_connectionString);
    }

    [Benchmark(Baseline = true, Description = "COUNT — UNION view")]
    public int Count_Union() =>
        ServiceQueueSchema.Count(_connectionString, "vw_ServiceQueue_Union", _serviceCenterId);

    [Benchmark(Description = "COUNT — UNION ALL view")]
    public int Count_UnionAll() =>
        ServiceQueueSchema.Count(_connectionString, "vw_ServiceQueue_UnionAll", _serviceCenterId);

    [Benchmark(Description = "Page 1 (50 rows) — UNION view")]
    public int Page_Union() =>
        ServiceQueueSchema.PagedQuery(_connectionString, "vw_ServiceQueue_Union", _serviceCenterId).Count;

    [Benchmark(Description = "Page 1 (50 rows) — UNION ALL view")]
    public int Page_UnionAll() =>
        ServiceQueueSchema.PagedQuery(_connectionString, "vw_ServiceQueue_UnionAll", _serviceCenterId).Count;

    [GlobalCleanup]
    public void GlobalCleanup() => ServiceQueueSchema.Teardown(_connectionString);
}

// ─── Domain model ─────────────────────────────────────────────────────────────

public record ServiceQueueRow(
    Guid     Id,
    string   DeviceSerial,
    string   DeviceType,
    string?  IssueDescription,
    DateTime DateCreated,
    string   ServiceCenter,
    int      ResolutionStatus,
    bool     IsAvailable,
    int      FormSource);

// ─── Schema, seeding, and query helpers ───────────────────────────────────────

public static class ServiceQueueSchema
{
    // Branch distribution mirrors real-world data:
    //   CustomerReturn  ~30 %, DirectExchange ~60 %,
    //   ServiceRequest   ~5 %, StoreReturn     ~5 %
    private const double FractionServiceRequest = 0.05;
    private const double FractionCustomerReturn = 0.30;
    private const double FractionDirectExchange = 0.60;
    // StoreReturn gets the remainder to ensure totals add up exactly

    // Proportion of forms assigned to the primary service center
    // (the one all benchmark queries filter on)
    private const double PrimaryFraction = 0.70;

    private static readonly string[] DeviceTypes =
        ["Smartphone", "Tablet", "Laptop", "Smart TV", "Wireless Earbuds", "Smartwatch", "Router"];

    private static readonly string[] Issues =
        ["Screen damage", "Battery failure", "Charging port fault", "Software crash",
         "Water damage", "Speaker fault", "Camera malfunction", "Overheating", "No power"];

    private static readonly string[] Technicians =
        ["Alice Brennan", "Bob Osei", "Carol Forde", "David Park", "Eve Mensah", "Frank Suleiman"];

    private static readonly string[] Cities =
        ["London", "Manchester", "Birmingham", "Glasgow", "Leeds"];

    // ── DDL ──────────────────────────────────────────────────────────────────

    private const string DropAndCreateTables = """
        IF OBJECT_ID('dbo.ServiceForms')     IS NOT NULL DROP TABLE dbo.ServiceForms;
        IF OBJECT_ID('dbo.ServiceRequests')  IS NOT NULL DROP TABLE dbo.ServiceRequests;
        IF OBJECT_ID('dbo.CustomerReturns')  IS NOT NULL DROP TABLE dbo.CustomerReturns;
        IF OBJECT_ID('dbo.DirectExchanges')  IS NOT NULL DROP TABLE dbo.DirectExchanges;
        IF OBJECT_ID('dbo.StoreReturns')     IS NOT NULL DROP TABLE dbo.StoreReturns;
        IF OBJECT_ID('dbo.ServiceCenters')   IS NOT NULL DROP TABLE dbo.ServiceCenters;

        CREATE TABLE dbo.ServiceCenters (
            Id   UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            Name NVARCHAR(200)    NOT NULL,
            City NVARCHAR(100)    NOT NULL
        );

        CREATE TABLE dbo.ServiceRequests (
            Id           UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            CustomerName NVARCHAR(200)    NOT NULL,
            ContactPhone NVARCHAR(20)     NULL,
            DateCreated  DATETIME         NOT NULL
        );

        CREATE TABLE dbo.CustomerReturns (
            Id             UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            OrderReference NVARCHAR(100)    NOT NULL,
            ReturnReason   NVARCHAR(500)    NULL,
            DateCreated    DATETIME         NOT NULL
        );

        CREATE TABLE dbo.DirectExchanges (
            Id                UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            ExchangeReference NVARCHAR(100)    NOT NULL,
            AuthorisedBy      NVARCHAR(200)    NULL,
            DateCreated       DATETIME         NOT NULL
        );

        CREATE TABLE dbo.StoreReturns (
            Id             UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            StoreCode      NVARCHAR(50)     NOT NULL,
            BatchReference NVARCHAR(100)    NULL,
            DateCreated    DATETIME         NOT NULL
        );

        -- ServiceForms is the central table. Each row links to exactly ONE
        -- source table via a nullable FK column — the others are always NULL.
        -- This one-of-four pattern is what makes UNION deduplication pointless.
        CREATE TABLE dbo.ServiceForms (
            Id                 UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
            DeviceSerial       NVARCHAR(100)    NOT NULL,
            DeviceType         NVARCHAR(200)    NOT NULL,
            IssueDescription   NVARCHAR(1000)   NULL,
            DateCreated        DATETIME         NOT NULL,
            DateResolved       DATETIME         NULL,
            ServiceCenterId    UNIQUEIDENTIFIER NOT NULL REFERENCES dbo.ServiceCenters(Id),
            AssignedTechnician NVARCHAR(200)    NULL,
            ResolutionStatus   INT              NOT NULL DEFAULT 0,  -- 0=Open 1=Resolved 2=Rejected
            IsAvailable        BIT              NOT NULL DEFAULT 1,
            OnServiceRequest   UNIQUEIDENTIFIER NULL REFERENCES dbo.ServiceRequests(Id),
            OnCustomerReturn   UNIQUEIDENTIFIER NULL REFERENCES dbo.CustomerReturns(Id),
            OnDirectExchange   UNIQUEIDENTIFIER NULL REFERENCES dbo.DirectExchanges(Id),
            OnStoreReturn      UNIQUEIDENTIFIER NULL REFERENCES dbo.StoreReturns(Id)
        );
        """;

    // Intentionally no index on ServiceCenterId — we want to observe the raw
    // UNION vs UNION ALL difference before any index optimization.

    private const string CreateUnionView = """
        CREATE OR ALTER VIEW dbo.vw_ServiceQueue_Union AS

        -- Branch 1: walk-in service requests
        SELECT SF.Id, SF.DeviceSerial, SF.DeviceType, SF.IssueDescription,
               SF.DateCreated, SF.DateResolved, SF.ServiceCenterId,
               SC.Name AS ServiceCenter, SF.AssignedTechnician,
               SF.ResolutionStatus, SF.IsAvailable, 1 AS FormSource
        FROM dbo.ServiceForms SF
        INNER JOIN dbo.ServiceRequests SR ON SF.OnServiceRequest = SR.Id
        INNER JOIN dbo.ServiceCenters  SC ON SF.ServiceCenterId  = SC.Id

        UNION  -- ← deduplicates the full combined result before any WHERE is applied

        -- Branch 2: online / in-store customer returns
        SELECT SF.Id, SF.DeviceSerial, SF.DeviceType, SF.IssueDescription,
               SF.DateCreated, SF.DateResolved, SF.ServiceCenterId,
               SC.Name AS ServiceCenter, SF.AssignedTechnician,
               SF.ResolutionStatus, SF.IsAvailable, 2 AS FormSource
        FROM dbo.ServiceForms SF
        INNER JOIN dbo.CustomerReturns CR ON SF.OnCustomerReturn = CR.Id
        INNER JOIN dbo.ServiceCenters  SC ON SF.ServiceCenterId  = SC.Id

        UNION

        -- Branch 3: direct device exchanges
        SELECT SF.Id, SF.DeviceSerial, SF.DeviceType, SF.IssueDescription,
               SF.DateCreated, SF.DateResolved, SF.ServiceCenterId,
               SC.Name AS ServiceCenter, SF.AssignedTechnician,
               SF.ResolutionStatus, SF.IsAvailable, 3 AS FormSource
        FROM dbo.ServiceForms SF
        INNER JOIN dbo.DirectExchanges DE ON SF.OnDirectExchange = DE.Id
        INNER JOIN dbo.ServiceCenters  SC ON SF.ServiceCenterId  = SC.Id

        UNION

        -- Branch 4: bulk store / outlet returns
        SELECT SF.Id, SF.DeviceSerial, SF.DeviceType, SF.IssueDescription,
               SF.DateCreated, SF.DateResolved, SF.ServiceCenterId,
               SC.Name AS ServiceCenter, SF.AssignedTechnician,
               SF.ResolutionStatus, SF.IsAvailable, 4 AS FormSource
        FROM dbo.ServiceForms SF
        INNER JOIN dbo.StoreReturns   STR ON SF.OnStoreReturn   = STR.Id
        INNER JOIN dbo.ServiceCenters SC  ON SF.ServiceCenterId = SC.Id;
        """;

    private const string CreateUnionAllView = """
        CREATE OR ALTER VIEW dbo.vw_ServiceQueue_UnionAll AS

        -- Branch 1: walk-in service requests
        SELECT SF.Id, SF.DeviceSerial, SF.DeviceType, SF.IssueDescription,
               SF.DateCreated, SF.DateResolved, SF.ServiceCenterId,
               SC.Name AS ServiceCenter, SF.AssignedTechnician,
               SF.ResolutionStatus, SF.IsAvailable, 1 AS FormSource
        FROM dbo.ServiceForms SF
        INNER JOIN dbo.ServiceRequests SR ON SF.OnServiceRequest = SR.Id
        INNER JOIN dbo.ServiceCenters  SC ON SF.ServiceCenterId  = SC.Id

        UNION ALL  -- ← no deduplication; predicates can be pushed into each branch

        -- Branch 2: online / in-store customer returns
        SELECT SF.Id, SF.DeviceSerial, SF.DeviceType, SF.IssueDescription,
               SF.DateCreated, SF.DateResolved, SF.ServiceCenterId,
               SC.Name AS ServiceCenter, SF.AssignedTechnician,
               SF.ResolutionStatus, SF.IsAvailable, 2 AS FormSource
        FROM dbo.ServiceForms SF
        INNER JOIN dbo.CustomerReturns CR ON SF.OnCustomerReturn = CR.Id
        INNER JOIN dbo.ServiceCenters  SC ON SF.ServiceCenterId  = SC.Id

        UNION ALL

        -- Branch 3: direct device exchanges
        SELECT SF.Id, SF.DeviceSerial, SF.DeviceType, SF.IssueDescription,
               SF.DateCreated, SF.DateResolved, SF.ServiceCenterId,
               SC.Name AS ServiceCenter, SF.AssignedTechnician,
               SF.ResolutionStatus, SF.IsAvailable, 3 AS FormSource
        FROM dbo.ServiceForms SF
        INNER JOIN dbo.DirectExchanges DE ON SF.OnDirectExchange = DE.Id
        INNER JOIN dbo.ServiceCenters  SC ON SF.ServiceCenterId  = SC.Id

        UNION ALL

        -- Branch 4: bulk store / outlet returns
        SELECT SF.Id, SF.DeviceSerial, SF.DeviceType, SF.IssueDescription,
               SF.DateCreated, SF.DateResolved, SF.ServiceCenterId,
               SC.Name AS ServiceCenter, SF.AssignedTechnician,
               SF.ResolutionStatus, SF.IsAvailable, 4 AS FormSource
        FROM dbo.ServiceForms SF
        INNER JOIN dbo.StoreReturns   STR ON SF.OnStoreReturn   = STR.Id
        INNER JOIN dbo.ServiceCenters SC  ON SF.ServiceCenterId = SC.Id;
        """;

    private const string DropEverything = """
        IF OBJECT_ID('dbo.vw_ServiceQueue_Union')    IS NOT NULL DROP VIEW dbo.vw_ServiceQueue_Union;
        IF OBJECT_ID('dbo.vw_ServiceQueue_UnionAll') IS NOT NULL DROP VIEW dbo.vw_ServiceQueue_UnionAll;
        IF OBJECT_ID('dbo.ServiceForms')             IS NOT NULL DROP TABLE dbo.ServiceForms;
        IF OBJECT_ID('dbo.ServiceRequests')          IS NOT NULL DROP TABLE dbo.ServiceRequests;
        IF OBJECT_ID('dbo.CustomerReturns')          IS NOT NULL DROP TABLE dbo.CustomerReturns;
        IF OBJECT_ID('dbo.DirectExchanges')          IS NOT NULL DROP TABLE dbo.DirectExchanges;
        IF OBJECT_ID('dbo.StoreReturns')             IS NOT NULL DROP TABLE dbo.StoreReturns;
        IF OBJECT_ID('dbo.ServiceCenters')           IS NOT NULL DROP TABLE dbo.ServiceCenters;
        """;

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>Creates schema and service centers. Returns the primary center ID used in queries.</summary>
    public static Guid Setup(string cs)
    {
        using var conn = new SqlConnection(cs);
        conn.Open();

        Exec(conn, DropAndCreateTables);
        Exec(conn, CreateUnionView);
        Exec(conn, CreateUnionAllView);

        var primaryId = Guid.NewGuid();
        InsertCenter(conn, primaryId, $"{Cities[0]} Central Repair Hub", Cities[0]);
        for (int i = 1; i < Cities.Length; i++)
            InsertCenter(conn, Guid.NewGuid(), $"{Cities[i]} Repair Hub", Cities[i]);

        return primaryId;
    }

    /// <summary>
    /// Seeds <paramref name="totalForms"/> ServiceForm rows distributed across
    /// four branches, with ~70 % assigned to <paramref name="primaryCenterId"/>.
    /// </summary>
    public static void Seed(string cs, int totalForms, Guid primaryCenterId)
    {
        var rng = new Random(42);

        int srCount = (int)(totalForms * FractionServiceRequest);
        int crCount = (int)(totalForms * FractionCustomerReturn);
        int deCount = (int)(totalForms * FractionDirectExchange);
        int stCount = totalForms - srCount - crCount - deCount;

        // Generate IDs for each branch source table
        var srIds = NewIds(srCount);
        var crIds = NewIds(crCount);
        var deIds = NewIds(deCount);
        var stIds = NewIds(stCount);

        // Bulk-insert branch source tables first (ServiceForms FKs reference them)
        BulkInsertServiceRequests(cs, srIds, rng);
        BulkInsertCustomerReturns(cs, crIds, rng);
        BulkInsertDirectExchanges(cs, deIds, rng);
        BulkInsertStoreReturns(cs, stIds, rng);

        // Resolve secondary center IDs for realistic data spread
        var secondaryIds = GetSecondaryCenterIds(cs, primaryCenterId);

        // Build ServiceForms DataTable — one row per source ID, exactly one FK set
        var dt = BuildServiceFormsTable();
        AppendForms(dt, srIds, "OnServiceRequest",  primaryCenterId, secondaryIds, rng);
        AppendForms(dt, crIds, "OnCustomerReturn",  primaryCenterId, secondaryIds, rng);
        AppendForms(dt, deIds, "OnDirectExchange",  primaryCenterId, secondaryIds, rng);
        AppendForms(dt, stIds, "OnStoreReturn",     primaryCenterId, secondaryIds, rng);

        BulkWrite(cs, "dbo.ServiceForms", dt);
    }

    /// <summary>Executes COUNT(*) against the given view filtered by service center and open status.</summary>
    public static int Count(string cs, string viewName, Guid serviceCenterId)
    {
        using var conn = new SqlConnection(cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT COUNT(*)
            FROM   dbo.[{viewName}]
            WHERE  ServiceCenterId  = @id
              AND  ResolutionStatus = 0
              AND  IsAvailable      = 1
            """;
        cmd.Parameters.AddWithValue("@id", serviceCenterId);
        return (int)cmd.ExecuteScalar()!;
    }

    /// <summary>Fetches the first page (50 rows) ordered by DateCreated DESC.</summary>
    public static List<ServiceQueueRow> PagedQuery(string cs, string viewName, Guid serviceCenterId)
    {
        using var conn = new SqlConnection(cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT TOP 50
                   Id, DeviceSerial, DeviceType, IssueDescription,
                   DateCreated, ServiceCenter, ResolutionStatus, IsAvailable, FormSource
            FROM   dbo.[{viewName}]
            WHERE  ServiceCenterId  = @id
              AND  ResolutionStatus = 0
              AND  IsAvailable      = 1
            ORDER BY DateCreated DESC
            """;
        cmd.Parameters.AddWithValue("@id", serviceCenterId);

        var rows = new List<ServiceQueueRow>(50);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new ServiceQueueRow(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetDateTime(4),
                reader.GetString(5),
                reader.GetInt32(6),
                reader.GetBoolean(7),
                reader.GetInt32(8)));
        }
        return rows;
    }

    /// <summary>
    /// Creates a composite index on ServiceForms that directly supports the
    /// benchmark query: WHERE ServiceCenterId = @id AND ResolutionStatus = 0
    /// AND IsAvailable = 1 ORDER BY DateCreated DESC.
    ///
    /// With UNION ALL this index enables an index seek per branch — SQL Server
    /// can push predicates into each branch individually and avoid touching
    /// unrelated rows entirely.
    ///
    /// With UNION this index still helps each branch seek, but deduplication
    /// requires SQL Server to materialise and hash/sort the full combined result
    /// before returning anything — so COUNT cannot short-circuit and a paged
    /// query must process far more rows than it returns.
    /// </summary>
    public static void CreateIndex(string cs)
    {
        using var conn = new SqlConnection(cs);
        conn.Open();
        Exec(conn, """
            CREATE INDEX IX_ServiceForms_QueueFilter
            ON dbo.ServiceForms (ServiceCenterId, ResolutionStatus, IsAvailable)
            INCLUDE (DateCreated, DeviceSerial, DeviceType, IssueDescription,
                     AssignedTechnician, DateResolved,
                     OnServiceRequest, OnCustomerReturn, OnDirectExchange, OnStoreReturn);
            """);
    }

    public static void Teardown(string cs)
    {
        using var conn = new SqlConnection(cs);
        conn.Open();
        Exec(conn, DropEverything);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static Guid[] NewIds(int count) =>
        Enumerable.Range(0, count).Select(_ => Guid.NewGuid()).ToArray();

    private static void InsertCenter(SqlConnection conn, Guid id, string name, string city)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO dbo.ServiceCenters (Id, Name, City) VALUES (@id, @name, @city)";
        cmd.Parameters.AddWithValue("@id",   id);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@city", city);
        cmd.ExecuteNonQuery();
    }

    private static void BulkInsertServiceRequests(string cs, Guid[] ids, Random rng)
    {
        var dt = new DataTable();
        dt.Columns.Add("Id",           typeof(Guid));
        dt.Columns.Add("CustomerName", typeof(string));
        dt.Columns.Add("ContactPhone", typeof(string));
        dt.Columns.Add("DateCreated",  typeof(DateTime));

        dt.BeginLoadData();
        foreach (var id in ids)
            dt.Rows.Add(id, $"Customer {id:N}", $"+44{rng.Next(1_000_000_000, 2_000_000_000)}", RandomDate(rng));
        dt.EndLoadData();

        BulkWrite(cs, "dbo.ServiceRequests", dt);
    }

    private static void BulkInsertCustomerReturns(string cs, Guid[] ids, Random rng)
    {
        var dt = new DataTable();
        dt.Columns.Add("Id",             typeof(Guid));
        dt.Columns.Add("OrderReference", typeof(string));
        dt.Columns.Add("ReturnReason",   typeof(string));
        dt.Columns.Add("DateCreated",    typeof(DateTime));

        dt.BeginLoadData();
        foreach (var id in ids)
            dt.Rows.Add(id, $"ORD-{rng.Next(100_000, 999_999)}", Issues[rng.Next(Issues.Length)], RandomDate(rng));
        dt.EndLoadData();

        BulkWrite(cs, "dbo.CustomerReturns", dt);
    }

    private static void BulkInsertDirectExchanges(string cs, Guid[] ids, Random rng)
    {
        var dt = new DataTable();
        dt.Columns.Add("Id",                typeof(Guid));
        dt.Columns.Add("ExchangeReference", typeof(string));
        dt.Columns.Add("AuthorisedBy",      typeof(string));
        dt.Columns.Add("DateCreated",       typeof(DateTime));

        dt.BeginLoadData();
        foreach (var id in ids)
            dt.Rows.Add(id, $"EXC-{rng.Next(100_000, 999_999)}", Technicians[rng.Next(Technicians.Length)], RandomDate(rng));
        dt.EndLoadData();

        BulkWrite(cs, "dbo.DirectExchanges", dt);
    }

    private static void BulkInsertStoreReturns(string cs, Guid[] ids, Random rng)
    {
        var dt = new DataTable();
        dt.Columns.Add("Id",             typeof(Guid));
        dt.Columns.Add("StoreCode",      typeof(string));
        dt.Columns.Add("BatchReference", typeof(string));
        dt.Columns.Add("DateCreated",    typeof(DateTime));

        dt.BeginLoadData();
        foreach (var id in ids)
            dt.Rows.Add(id, $"STORE-{rng.Next(10, 99)}", $"BATCH-{rng.Next(1_000, 9_999)}", RandomDate(rng));
        dt.EndLoadData();

        BulkWrite(cs, "dbo.StoreReturns", dt);
    }

    private static DataTable BuildServiceFormsTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("Id",                 typeof(Guid));
        dt.Columns.Add("DeviceSerial",       typeof(string));
        dt.Columns.Add("DeviceType",         typeof(string));
        dt.Columns.Add("IssueDescription",   typeof(string));
        dt.Columns.Add("DateCreated",        typeof(DateTime));
        dt.Columns.Add("DateResolved",       typeof(DateTime));  // allows DBNull
        dt.Columns.Add("ServiceCenterId",    typeof(Guid));
        dt.Columns.Add("AssignedTechnician", typeof(string));    // allows DBNull
        dt.Columns.Add("ResolutionStatus",   typeof(int));
        dt.Columns.Add("IsAvailable",        typeof(bool));
        dt.Columns.Add("OnServiceRequest",   typeof(Guid));      // allows DBNull
        dt.Columns.Add("OnCustomerReturn",   typeof(Guid));      // allows DBNull
        dt.Columns.Add("OnDirectExchange",   typeof(Guid));      // allows DBNull
        dt.Columns.Add("OnStoreReturn",      typeof(Guid));      // allows DBNull
        return dt;
    }

    private static void AppendForms(
        DataTable dt,
        Guid[]    sourceIds,
        string    fkColumn,
        Guid      primaryCenterId,
        Guid[]    secondaryIds,
        Random    rng)
    {
        dt.BeginLoadData();
        foreach (var sourceId in sourceIds)
        {
            var centerId = rng.NextDouble() < PrimaryFraction
                ? primaryCenterId
                : secondaryIds[rng.Next(secondaryIds.Length)];

            var created  = RandomDate(rng);
            var resolved = rng.NextDouble() > 0.7
                ? (object)created.AddDays(rng.Next(1, 14))
                : DBNull.Value;

            var row = dt.NewRow();
            row["Id"]                 = Guid.NewGuid();
            row["DeviceSerial"]       = $"DEV-{rng.Next(1_000_000, 9_999_999)}";
            row["DeviceType"]         = DeviceTypes[rng.Next(DeviceTypes.Length)];
            row["IssueDescription"]   = Issues[rng.Next(Issues.Length)];
            row["DateCreated"]        = created;
            row["DateResolved"]       = resolved;
            row["ServiceCenterId"]    = centerId;
            row["AssignedTechnician"] = rng.NextDouble() > 0.3
                ? Technicians[rng.Next(Technicians.Length)]
                : DBNull.Value;
            row["ResolutionStatus"]   = rng.Next(0, 3);
            row["IsAvailable"]        = rng.NextDouble() > 0.2;

            // Set exactly one FK column; all others remain DBNull
            row["OnServiceRequest"] = DBNull.Value;
            row["OnCustomerReturn"] = DBNull.Value;
            row["OnDirectExchange"] = DBNull.Value;
            row["OnStoreReturn"]    = DBNull.Value;
            row[fkColumn]           = sourceId;

            dt.Rows.Add(row);
        }
        dt.EndLoadData();
    }

    private static Guid[] GetSecondaryCenterIds(string cs, Guid primaryId)
    {
        using var conn = new SqlConnection(cs);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM dbo.ServiceCenters WHERE Id <> @id";
        cmd.Parameters.AddWithValue("@id", primaryId);
        using var reader = cmd.ExecuteReader();
        var ids = new List<Guid>();
        while (reader.Read()) ids.Add(reader.GetGuid(0));
        return ids.ToArray();
    }

    private static void BulkWrite(string cs, string tableName, DataTable dt)
    {
        using var conn = new SqlConnection(cs);
        conn.Open();
        using var bulk = new SqlBulkCopy(conn)
        {
            DestinationTableName = tableName,
            BatchSize            = 10_000,
            BulkCopyTimeout      = 600
        };
        bulk.WriteToServer(dt);
    }

    private static void Exec(SqlConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText    = sql;
        cmd.CommandTimeout = 300;
        cmd.ExecuteNonQuery();
    }

    private static DateTime RandomDate(Random rng) =>
        new DateTime(2021, 1, 1).AddDays(rng.Next(0, 3 * 365));
}
