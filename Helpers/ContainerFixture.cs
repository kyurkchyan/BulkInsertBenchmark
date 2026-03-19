using Testcontainers.MsSql;

namespace BulkInsertBenchmark.Helpers;

public static class ContainerFixture
{
    private static MsSqlContainer? _container;

    public static string ConnectionString { get; private set; } = string.Empty;

    public static async Task StartAsync()
    {
        Console.WriteLine("Starting SQL Server container (requires Docker)...");
        _container = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();

        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
        Console.WriteLine("SQL Server container ready.\n");
    }

    public static async Task StopAsync()
    {
        if (_container is not null)
        {
            await _container.StopAsync();
            await _container.DisposeAsync();
        }
    }
}
