using BulkInsertBenchmark.Models;

namespace BulkInsertBenchmark.Helpers;

public static class DataGenerator
{
    private static readonly string[] Statuses = ["Pending", "Completed", "Cancelled", "Processing", "Shipped"];
    private static readonly Random Rng = new(42); // fixed seed for reproducibility

    public static List<Order> Generate(int count)
    {
        var orders = new List<Order>(count);
        var baseDate = new DateTime(2020, 1, 1);

        for (int i = 0; i < count; i++)
        {
            orders.Add(new Order
            {
                CustomerName = $"Customer_{Rng.Next(1, 100_000):D6}",
                ProductCode = $"PROD_{Rng.Next(1, 10_000):D4}",
                OrderDate = baseDate.AddDays(Rng.Next(0, 1825)),
                TotalAmount = Math.Round((decimal)(Rng.NextDouble() * 9999 + 1), 2),
                Quantity = Rng.Next(1, 100),
                Status = Statuses[Rng.Next(Statuses.Length)]
            });
        }

        return orders;
    }
}
