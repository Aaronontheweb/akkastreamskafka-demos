namespace Shared;

public record OrderEvent(
    string OrderId,
    string CustomerId,
    decimal Amount,
    DateTime Timestamp,
    string ProductId
);

public record ProcessedOrder(
    string OrderId,
    string CustomerId,
    decimal Amount,
    DateTime ProcessedAt,
    ProcessingStatus Status,
    string? ErrorMessage = null
);

public enum ProcessingStatus
{
    Success,
    Failed,
    Retry,
    DeadLettered
}

public static class Topics
{
    public const string Orders = "orders";
    public const string ProcessedOrders = "processed-orders";
    public const string DeadLetters = "orders-dlq";
}

public class ProcessingException : Exception
{
    public bool IsTransient { get; }
    
    public ProcessingException(string message, bool isTransient = true) 
        : base(message)
    {
        IsTransient = isTransient;
    }
}

public class PoisonMessageException : ProcessingException
{
    public PoisonMessageException(string message) 
        : base(message, isTransient: false)
    {
    }
}