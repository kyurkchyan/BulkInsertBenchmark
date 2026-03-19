# BulkInsertBenchmark

A .NET 8 benchmark comparing the performance of different bulk insert strategies into SQL Server using [BenchmarkDotNet](https://benchmarkdotnet.org/).

## What it benchmarks

Two approaches are compared:

| Benchmark | Description |
|---|---|
| `SqlBulkCopy` *(baseline)* | ADO.NET `SqlBulkCopy` with a `DataTable`, batched at 10,000 rows, wrapped in a transaction |
| `EFCore.BulkExtensions` | [`EFCore.BulkExtensions`](https://github.com/borisdj/EFCore.BulkExtensions) `BulkInsert`, batched at 10,000 rows |

The test entity is an `Order` with 6 fields (string, DateTime, decimal, int).

## Prerequisites

- .NET 8 SDK
- Docker (SQL Server is spun up automatically via [Testcontainers](https://dotnet.testcontainers.org/) — no manual SQL Server installation needed)

## Running

```bash
# Quick timed comparison — 1,000,000 records (default)
dotnet run -c Release

# Custom record count
dotnet run -c Release -- 5000000

# Full BenchmarkDotNet run — 10K / 100K / 500K, 3 iterations each
dotnet run -c Release -- --benchmark
```

The database and `Orders` table are created automatically on first run. The Docker container is started and torn down automatically each run.

## Results

Measured on a WSL2/Docker environment (93 GB RAM, SQL Server 2022).

### 1,000,000 records

| Method | Time | Throughput |
|---|---|---|
| `SqlBulkCopy` | 7.47s | 133,939 rec/sec |
| `EFCore.BulkExtensions` | **6.60s** | **151,514 rec/sec** |

### 10,000,000 records

| Method | Time | Throughput |
|---|---|---|
| `SqlBulkCopy` | **60.46s** | **165,406 rec/sec** |
| `EFCore.BulkExtensions` | 65.38s | 152,962 rec/sec |

**Takeaway:** `EFCore.BulkExtensions` has a slight edge at lower record counts (avoids materialising a `DataTable`), while raw `SqlBulkCopy` pulls ahead at 10M+ rows where its lower per-batch overhead amortises the `DataTable` cost.

## Dependencies

- [BenchmarkDotNet](https://www.nuget.org/packages/BenchmarkDotNet) `0.14.0`
- [Microsoft.EntityFrameworkCore.SqlServer](https://www.nuget.org/packages/Microsoft.EntityFrameworkCore.SqlServer) `8.0.13`
- [EFCore.BulkExtensions](https://www.nuget.org/packages/EFCore.BulkExtensions) `8.1.3`
- [Testcontainers.MsSql](https://www.nuget.org/packages/Testcontainers.MsSql) `3.10.0`
