# BulkInsertBenchmark

A .NET 8 benchmark comparing the performance of different bulk insert strategies into SQL Server using [BenchmarkDotNet](https://benchmarkdotnet.org/).

## What it benchmarks

Two approaches are compared across record counts of **10,000**, **100,000**, and **500,000** rows:

| Benchmark | Description |
|---|---|
| `SqlBulkCopy` *(baseline)* | ADO.NET `SqlBulkCopy` with a `DataTable`, batched at 10,000 rows, wrapped in a transaction |
| `EFCore.BulkExtensions` | [`EFCore.BulkExtensions`](https://github.com/borisdj/EFCore.BulkExtensions) `BulkInsert`, batched at 10,000 rows |

The test entity is an `Order` with 6 fields (string, DateTime, decimal, int).

## Prerequisites

- .NET 8 SDK
- SQL Server instance (local or remote)

## Setup

1. Clone the repo:
   ```bash
   git clone https://github.com/kyurkchyan/BulkInsertBenchmark.git
   cd BulkInsertBenchmark
   ```

2. Set your connection string in `appsettings.json`:
   ```json
   {
     "ConnectionString": "Server=.;Database=BulkInsertBenchmark;Trusted_Connection=True;TrustServerCertificate=True;"
   }
   ```

3. The database and `Orders` table are created automatically on first run via `EnsureCreated()`.

## Running the benchmarks

```bash
dotnet run -c Release
```

BenchmarkDotNet requires a **Release** build to produce reliable results.

## Dependencies

- [BenchmarkDotNet](https://www.nuget.org/packages/BenchmarkDotNet) `0.14.0`
- [Microsoft.EntityFrameworkCore.SqlServer](https://www.nuget.org/packages/Microsoft.EntityFrameworkCore.SqlServer) `8.0.13`
- [EFCore.BulkExtensions](https://www.nuget.org/packages/EFCore.BulkExtensions) `8.1.3`
- [Microsoft.Extensions.Configuration.Json](https://www.nuget.org/packages/Microsoft.Extensions.Configuration.Json) `8.0.1`
