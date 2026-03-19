using BulkInsertBenchmark.Models;
using Microsoft.EntityFrameworkCore;

namespace BulkInsertBenchmark.Data;

public class AppDbContext : DbContext
{
    private readonly string _connectionString;

    public AppDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseSqlServer(_connectionString)
                  .EnableSensitiveDataLogging(false)
                  .LogTo(_ => { }); // suppress EF Core logging during benchmarks

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.CustomerName).HasMaxLength(100).IsRequired();
            entity.Property(e => e.ProductCode).HasMaxLength(50).IsRequired();
            entity.Property(e => e.Status).HasMaxLength(20).IsRequired();
            entity.Property(e => e.TotalAmount).HasColumnType("decimal(18,2)");
        });
    }
}
