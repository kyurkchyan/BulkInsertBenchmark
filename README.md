# BulkInsertBenchmark

A .NET 8 benchmark comparing the performance of different bulk insert strategies into SQL Server using [BenchmarkDotNet](https://benchmarkdotnet.org/).

## What it benchmarks

### Insert

| Benchmark | Description |
|---|---|
| `SqlBulkCopy` *(baseline)* | ADO.NET `SqlBulkCopy` with a `DataTable`, batched at 10,000 rows, wrapped in a transaction |
| `EFCore.BulkExtensions` | [`EFCore.BulkExtensions`](https://github.com/borisdj/EFCore.BulkExtensions) `BulkInsert`, batched at 10,000 rows |

### Update & Delete

| Benchmark | Description |
|---|---|
| `Loop + SaveChanges` *(baseline)* | Load all entities into memory with change tracking, mutate each one, call `SaveChanges` |
| `ExecuteUpdateAsync` | EF Core 7+ set-based update — single SQL `UPDATE`, no entity loading |
| `ExecuteDeleteAsync` | EF Core 7+ set-based delete — single SQL `DELETE`, no entity loading |

The test entity is an `Order` with 6 fields (string, DateTime, decimal, int).

## Prerequisites

- .NET 8 SDK
- Docker (SQL Server is spun up automatically via [Testcontainers](https://dotnet.testcontainers.org/) — no manual SQL Server installation needed)

## Running

```bash
# Insert comparison — 1,000,000 records (default)
dotnet run -c Release

# Insert comparison — custom record count
dotnet run -c Release -- 5000000

# Update comparison — 100,000 records (default)
dotnet run -c Release -- update

# Update comparison — custom record count
dotnet run -c Release -- update 500000

# Delete comparison — 100,000 records (default)
dotnet run -c Release -- delete

# Full BenchmarkDotNet run (Insert + Update + Delete, 10K/50K/100K, 3 iterations)
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

### UPDATE — 100,000 records

| Method | Time | Throughput | Ratio |
|---|---|---|---|
| `Loop + SaveChanges` | 85.44s | 1,170 rec/sec | 191× slower |
| `ExecuteUpdateAsync` | **0.45s** | **223,832 rec/sec** | — |

### DELETE — 100,000 records

| Method | Time | Throughput | Ratio |
|---|---|---|---|
| `Loop + SaveChanges` | 5.05s | 19,800 rec/sec | 25× slower |
| `ExecuteDeleteAsync` | **0.20s** | **511,666 rec/sec** | — |

**Takeaway:** `ExecuteUpdateAsync` is **191× faster** than the loop approach for updates — loading 100K entities into the change tracker and issuing per-row `UPDATE` statements dominates the cost. `ExecuteDeleteAsync` is **25× faster**; the loop is less extreme because EF Core batches deletes more aggressively than updates.

## Dependencies

- [BenchmarkDotNet](https://www.nuget.org/packages/BenchmarkDotNet) `0.14.0`
- [Microsoft.EntityFrameworkCore.SqlServer](https://www.nuget.org/packages/Microsoft.EntityFrameworkCore.SqlServer) `8.0.13`
- [EFCore.BulkExtensions](https://www.nuget.org/packages/EFCore.BulkExtensions) `8.1.3`
- [Testcontainers.MsSql](https://www.nuget.org/packages/Testcontainers.MsSql) `3.10.0`
