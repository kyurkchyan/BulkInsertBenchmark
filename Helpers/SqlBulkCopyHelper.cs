using BulkInsertBenchmark.Models;
using Microsoft.Data.SqlClient;
using System.Data;

namespace BulkInsertBenchmark.Helpers;

public static class SqlBulkCopyHelper
{
    private const int BatchSize = 10_000;

    public static void BulkInsert(string connectionString, List<Order> orders)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        try
        {
            using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = "Orders",
                BatchSize = BatchSize,
                BulkCopyTimeout = 600
            };

            // Map columns — Id is identity so we skip it
            bulkCopy.ColumnMappings.Add(nameof(Order.CustomerName), "CustomerName");
            bulkCopy.ColumnMappings.Add(nameof(Order.ProductCode), "ProductCode");
            bulkCopy.ColumnMappings.Add(nameof(Order.OrderDate), "OrderDate");
            bulkCopy.ColumnMappings.Add(nameof(Order.TotalAmount), "TotalAmount");
            bulkCopy.ColumnMappings.Add(nameof(Order.Quantity), "Quantity");
            bulkCopy.ColumnMappings.Add(nameof(Order.Status), "Status");

            using var dataTable = ToDataTable(orders);
            bulkCopy.WriteToServer(dataTable);

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    private static DataTable ToDataTable(List<Order> orders)
    {
        var table = new DataTable();
        table.Columns.Add(nameof(Order.CustomerName), typeof(string));
        table.Columns.Add(nameof(Order.ProductCode), typeof(string));
        table.Columns.Add(nameof(Order.OrderDate), typeof(DateTime));
        table.Columns.Add(nameof(Order.TotalAmount), typeof(decimal));
        table.Columns.Add(nameof(Order.Quantity), typeof(int));
        table.Columns.Add(nameof(Order.Status), typeof(string));

        table.BeginLoadData();
        foreach (var o in orders)
        {
            table.Rows.Add(o.CustomerName, o.ProductCode, o.OrderDate, o.TotalAmount, o.Quantity, o.Status);
        }
        table.EndLoadData();

        return table;
    }

    public static void TruncateTable(string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "TRUNCATE TABLE Orders";
        cmd.ExecuteNonQuery();
    }
}
