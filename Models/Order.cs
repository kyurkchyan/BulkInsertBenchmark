namespace BulkInsertBenchmark.Models;

public class Order
{
    public int Id { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public string ProductCode { get; set; } = string.Empty;
    public DateTime OrderDate { get; set; }
    public decimal TotalAmount { get; set; }
    public int Quantity { get; set; }
    public string Status { get; set; } = string.Empty;
}
