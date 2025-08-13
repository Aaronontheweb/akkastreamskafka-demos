using System.Collections.Concurrent;

namespace Shared;

public class OrderProcessor
{
    private readonly Random _random = new();
    private readonly ConcurrentDictionary<string, int> _attemptCounts = new();
    
    // Simulate different failure scenarios
    private readonly HashSet<string> _poisonOrderIds = new()
    {
        "POISON-001",
        "POISON-002"
    };
    
    private readonly HashSet<string> _transientFailureOrderIds = new()
    {
        "TRANSIENT-001",
        "TRANSIENT-002",
        "TRANSIENT-003"
    };

    public async Task<ProcessedOrder> ProcessOrderAsync(OrderEvent order)
    {
        // Track attempt count
        var attempts = _attemptCounts.AddOrUpdate(order.OrderId, 1, (_, count) => count + 1);
        
        Console.WriteLine($"[Attempt {attempts}] Processing order {order.OrderId} for ${order.Amount:F2}");
        
        // Simulate processing delay
        await Task.Delay(100 + _random.Next(200));
        
        // Simulate different scenarios
        if (_poisonOrderIds.Contains(order.OrderId))
        {
            throw new PoisonMessageException($"Order {order.OrderId} contains invalid data that cannot be processed");
        }
        
        if (_transientFailureOrderIds.Contains(order.OrderId))
        {
            // Fail first 2 attempts, succeed on 3rd
            if (attempts < 3)
            {
                throw new ProcessingException($"Database timeout for order {order.OrderId} (attempt {attempts})", isTransient: true);
            }
        }
        
        // Random transient failures (10% chance)
        if (_random.Next(10) == 0 && attempts == 1)
        {
            throw new ProcessingException("Service temporarily unavailable", isTransient: true);
        }
        
        // Business validation
        if (order.Amount <= 0)
        {
            throw new PoisonMessageException($"Invalid order amount: {order.Amount}");
        }
        
        if (order.Amount > 10000)
        {
            // Large orders need additional verification
            await Task.Delay(500);
            Console.WriteLine($"  ✓ Large order {order.OrderId} verified");
        }
        
        // Success!
        return new ProcessedOrder(
            order.OrderId,
            order.CustomerId,
            order.Amount,
            DateTime.UtcNow,
            ProcessingStatus.Success
        );
    }
    
    public void Reset()
    {
        _attemptCounts.Clear();
    }
}