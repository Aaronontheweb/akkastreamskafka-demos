namespace Shared;

public class DemoDataGenerator
{
    private readonly Random _random = new();
    
    private readonly string[] _customerIds = 
    {
        "CUST-001", "CUST-002", "CUST-003", "CUST-004", "CUST-005"
    };
    
    private readonly string[] _productIds = 
    {
        "PROD-A", "PROD-B", "PROD-C", "PROD-D", "PROD-E"
    };

    public IEnumerable<OrderEvent> GenerateOrders(int count = 100)
    {
        for (int i = 0; i < count; i++)
        {
            yield return GenerateOrder(i);
        }
    }
    
    private OrderEvent GenerateOrder(int index)
    {
        // Insert some poison messages
        if (index == 15)
            return new OrderEvent("POISON-001", "CUST-POISON", -100, DateTime.UtcNow, "INVALID");
        if (index == 30)
            return new OrderEvent("POISON-002", "CUST-POISON", 0, DateTime.UtcNow, "INVALID");
            
        // Insert some transient failure messages
        if (index == 10)
            return new OrderEvent("TRANSIENT-001", _customerIds[0], 150.00m, DateTime.UtcNow, _productIds[0]);
        if (index == 20)
            return new OrderEvent("TRANSIENT-002", _customerIds[1], 250.00m, DateTime.UtcNow, _productIds[1]);
        if (index == 40)
            return new OrderEvent("TRANSIENT-003", _customerIds[2], 350.00m, DateTime.UtcNow, _productIds[2]);
        
        // Normal orders
        var orderId = $"ORDER-{index:D6}";
        var customerId = _customerIds[_random.Next(_customerIds.Length)];
        var productId = _productIds[_random.Next(_productIds.Length)];
        var amount = Math.Round((decimal)(_random.NextDouble() * 1000 + 10), 2);
        
        // Occasionally create large orders
        if (_random.Next(20) == 0)
        {
            amount = Math.Round((decimal)(_random.NextDouble() * 10000 + 10000), 2);
        }
        
        return new OrderEvent(orderId, customerId, amount, DateTime.UtcNow, productId);
    }
}