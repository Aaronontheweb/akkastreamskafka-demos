using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Shared;

// Parse command line args
var instanceId = args.Length > 0 ? args[0] : "1";
Console.Title = $"Confluent.Kafka Instance {instanceId}";

// Setup simple console logging
using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

var logger = loggerFactory.CreateLogger("ConfluentKafka.LowLevel");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

// Simple bootstrap servers - either local or from environment variable
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
logger.LogInformation("Using Kafka at: {BootstrapServers}", bootstrapServers);

// Check if Kafka is available
if (!KafkaHelper.CheckKafkaAvailability(bootstrapServers))
{
    logger.LogError("Failed to connect to Kafka");
    Environment.Exit(1);
}

logger.LogInformation("============================================");
logger.LogInformation("CONFLUENT.KAFKA - INSTANCE {InstanceId}", instanceId);
logger.LogInformation("============================================");
logger.LogInformation("🔴 MANUAL ERROR HANDLING & REBALANCING 🔴");
logger.LogInformation("Watch the complexity of:");
logger.LogInformation("- Retry logic with exponential backoff");
logger.LogInformation("- Dead letter queue management");
logger.LogInformation("- Thread pool coordination");
logger.LogInformation("- Manual offset management");
logger.LogInformation("- Partition rebalancing callbacks");
logger.LogInformation("");
logger.LogInformation("💡 TIP: Run multiple instances to see rebalancing chaos!");
logger.LogInformation("   dotnet run 1  (first instance)");
logger.LogInformation("   dotnet run 2  (second instance)");
logger.LogInformation("   dotnet run 3  (third instance)");
logger.LogInformation("Press Ctrl+C to stop...");

// Start the consumer
var consumer = new ManualErrorHandlingConsumer(bootstrapServers, instanceId, logger);
await consumer.StartAsync(cts.Token);

logger.LogInformation("✓ Consumer stopped gracefully");

// Reset terminal title
Console.Title = "";

public class ManualErrorHandlingConsumer
{
    private readonly string _bootstrapServers;
    private readonly string _instanceId;
    private readonly ILogger _logger;
    private readonly OrderProcessor _processor = new();
    private readonly ConcurrentDictionary<string, RetryInfo> _retryTracker = new();
    private readonly ConcurrentQueue<(OrderEvent order, int attempts, Exception error)> _deadLetterQueue = new();
    private readonly SemaphoreSlim _processingThrottle = new(10);
    private readonly object _offsetLock = new();
    private readonly Dictionary<TopicPartition, Offset> _pendingOffsets = new();
    private readonly HashSet<TopicPartition> _assignedPartitions = new();
    
    public ManualErrorHandlingConsumer(string bootstrapServers, string instanceId, ILogger logger)
    {
        _bootstrapServers = bootstrapServers;
        _instanceId = instanceId;
        _logger = logger;
    }
    
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "manual-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            SessionTimeoutMs = 6000,
            HeartbeatIntervalMs = 2000
        };
        
        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation("[REBALANCE-{InstanceId}] ASSIGNED: {Partitions}", 
                    _instanceId, string.Join(", ", partitions.Select(p => $"P{p.Partition.Value}")));
                
                // Complex state management during rebalancing
                lock (_assignedPartitions)
                {
                    _assignedPartitions.Clear();
                    foreach (var tp in partitions)
                    {
                        _assignedPartitions.Add(new TopicPartition(tp.Topic, tp.Partition));
                    }
                }
                
                // Reset position to stored offset or beginning
                var offsets = partitions.Select(tp => new TopicPartitionOffset(tp, Offset.Unset));
                return offsets;
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _logger.LogInformation("[REBALANCE-{InstanceId}] REVOKED: {Partitions}", 
                    _instanceId, string.Join(", ", partitions.Select(p => $"P{p.Partition.Value}")));
                
                // Try to commit any pending offsets before revocation
                try
                {
                    c.Commit();
                }
                catch (KafkaException ex)
                {
                    _logger.LogError(ex, "Failed to commit during revocation");
                }
                
                lock (_assignedPartitions)
                {
                    foreach (var tp in partitions)
                    {
                        _assignedPartitions.Remove(new TopicPartition(tp.Topic, tp.Partition));
                    }
                }
            })
            .SetPartitionsLostHandler((c, partitions) =>
            {
                _logger.LogWarning("[REBALANCE-{InstanceId}] LOST: {Partitions} - potential data loss!", 
                    _instanceId, string.Join(", ", partitions.Select(p => $"P{p.Partition.Value}")));
                
                lock (_assignedPartitions)
                {
                    foreach (var tp in partitions)
                    {
                        _assignedPartitions.Remove(new TopicPartition(tp.Topic, tp.Partition));
                    }
                }
            })
            .SetErrorHandler((c, error) =>
            {
                _logger.LogError("[ERROR-{InstanceId}] {Reason}", _instanceId, error.Reason);
            })
            .Build();
            
        consumer.Subscribe(Topics.Orders);
        
        var processingTasks = new List<Task>();
        var dlqTask = Task.Run(() => ProcessDeadLetterQueue(cancellationToken), cancellationToken);
        
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    
                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                        continue;
                    
                    // Check if we still own this partition (rebalancing race condition)
                    lock (_assignedPartitions)
                    {
                        if (!_assignedPartitions.Contains(new TopicPartition(consumeResult.Topic, consumeResult.Partition)))
                        {
                            _logger.LogWarning("[{InstanceId}] Received message from unassigned partition {Partition}", 
                                _instanceId, consumeResult.TopicPartition);
                            continue;
                        }
                    }
                    
                    // Deserialize message
                    OrderEvent? order;
                    try
                    {
                        order = JsonSerializer.Deserialize<OrderEvent>(consumeResult.Message.Value);
                        if (order == null) throw new InvalidOperationException("Null order");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "[{InstanceId}] Failed to deserialize message", _instanceId);
                        consumer.Commit(consumeResult);
                        continue;
                    }
                    
                    _logger.LogInformation("[{InstanceId}:P{Partition}] Processing {OrderId}", 
                        _instanceId, consumeResult.Partition.Value, order.OrderId);
                    
                    // Process with complex error handling
                    _logger.LogDebug("[{InstanceId}:P{Partition}] Acquiring processing semaphore (current count: {Count}/10)", 
                        _instanceId, consumeResult.Partition.Value, 10 - _processingThrottle.CurrentCount);
                    await _processingThrottle.WaitAsync(cancellationToken);
                    _logger.LogDebug("[{InstanceId}:P{Partition}] Semaphore acquired, spawning processing task", 
                        _instanceId, consumeResult.Partition.Value);
                    
                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessWithRetryAsync(order, consumeResult, consumer);
                        }
                        finally
                        {
                            _processingThrottle.Release();
                        }
                    }, cancellationToken);
                    
                    processingTasks.Add(task);
                    processingTasks.RemoveAll(t => t.IsCompleted);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "[{InstanceId}] Consume error", _instanceId);
                }
            }
            
            await Task.WhenAll(processingTasks);
            await dlqTask;
            consumer.Close();
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }
    
    private async Task ProcessWithRetryAsync(
        OrderEvent order,
        ConsumeResult<string, string> consumeResult,
        IConsumer<string, string> consumer)
    {
        var retryInfo = _retryTracker.GetOrAdd(order.OrderId, _ => new RetryInfo());
        
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                if (attempt > 1)
                {
                    var delay = TimeSpan.FromSeconds(Math.Pow(2, attempt - 1));
                    _logger.LogWarning("  [{InstanceId}] 🔄 RETRY ATTEMPT {Attempt}/3 for {OrderId} after {Delay}s backoff", 
                        _instanceId, attempt, order.OrderId, delay.TotalSeconds);
                    _logger.LogInformation("  [{InstanceId}] ⏱️ Waiting {Delay}s before retry (thread blocked)...", 
                        _instanceId, delay.TotalSeconds);
                    await Task.Delay(delay);
                    _logger.LogInformation("  [{InstanceId}] ▶️ Resuming processing after backoff", _instanceId);
                }
                
                var result = await _processor.ProcessOrderAsync(order);
                _logger.LogInformation("  [{InstanceId}] ✓ Processed {OrderId}", _instanceId, order.OrderId);
                
                // Manual offset management with locking
                lock (_offsetLock)
                {
                    var tp = new TopicPartition(consumeResult.Topic, consumeResult.Partition);
                    _pendingOffsets[tp] = consumeResult.Offset + 1;
                    
                    _logger.LogDebug("  [{InstanceId}] 🔒 Acquired offset lock for partition {Partition}", 
                        _instanceId, consumeResult.Partition.Value);
                    
                    try
                    {
                        consumer.Commit(_pendingOffsets.Select(kv => 
                            new TopicPartitionOffset(kv.Key, kv.Value)));
                        _logger.LogDebug("  [{InstanceId}] ✓ Committed offset {Offset} for partition {Partition}", 
                            _instanceId, consumeResult.Offset.Value, consumeResult.Partition.Value);
                    }
                    catch (KafkaException ex)
                    {
                        _logger.LogError(ex, "[{InstanceId}] ❌ COMMIT FAILED - POTENTIAL MESSAGE LOSS OR DUPLICATION for offset {Offset}, partition {Partition}", 
                            _instanceId, consumeResult.Offset.Value, consumeResult.Partition.Value);
                        _logger.LogError("  [{InstanceId}] ⚠️ Manual intervention may be required to recover from this state", _instanceId);
                    }
                    finally
                    {
                        _logger.LogDebug("  [{InstanceId}] 🔓 Released offset lock", _instanceId);
                    }
                }
                
                _retryTracker.TryRemove(order.OrderId, out _);
                return;
            }
            catch (PoisonMessageException ex)
            {
                _logger.LogError(ex, "  [{InstanceId}] 🚫 POISON MESSAGE DETECTED: {OrderId}", _instanceId, order.OrderId);
                _logger.LogError("  [{InstanceId}] 💀 Moving to DLQ immediately - no retry for poison messages", _instanceId);
                _logger.LogWarning("  [{InstanceId}] ⚠️ This message will never be processed successfully", _instanceId);
                _deadLetterQueue.Enqueue((order, attempt, ex));
                CommitOffsetSafely(consumer, consumeResult);
                return;
            }
            catch (ProcessingException ex) when (ex.IsTransient && attempt < 3)
            {
                _logger.LogWarning(ex, "  [{InstanceId}] ⚠️ TRANSIENT ERROR on attempt {Attempt}/3 for {OrderId}: {Message}", 
                    _instanceId, attempt, order.OrderId, ex.Message);
                _logger.LogWarning("  [{InstanceId}] 🔄 Initiating retry with exponential backoff...", _instanceId);
                _logger.LogDebug("  [{InstanceId}] Stack trace: {StackTrace}", _instanceId, ex.StackTrace);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "  [{InstanceId}] ❌ UNEXPECTED ERROR on attempt {Attempt}/3 for {OrderId}", 
                    _instanceId, attempt, order.OrderId);
                
                if (attempt == 3)
                {
                    _logger.LogError("  [{InstanceId}] 💀 GIVING UP after 3 attempts - moving {OrderId} to DLQ", 
                        _instanceId, order.OrderId);
                    _logger.LogError("  [{InstanceId}] ⚠️ Message will be lost from main processing flow", _instanceId);
                    _deadLetterQueue.Enqueue((order, attempt, ex));
                    CommitOffsetSafely(consumer, consumeResult);
                }
                else
                {
                    _logger.LogWarning("  [{InstanceId}] Will retry {OrderId} (attempt {NextAttempt}/3)", 
                        _instanceId, order.OrderId, attempt + 1);
                }
            }
        }
    }
    
    private void CommitOffsetSafely(IConsumer<string, string> consumer, ConsumeResult<string, string> result)
    {
        try
        {
            _logger.LogDebug("[{InstanceId}] Attempting to commit offset {Offset} for partition {Partition}", 
                _instanceId, result.Offset.Value, result.Partition.Value);
            consumer.Commit(result);
            _logger.LogDebug("[{InstanceId}] ✓ Successfully committed offset", _instanceId);
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "[{InstanceId}] ❌ COMMIT FAILED - RISK OF MESSAGE REPROCESSING on restart", _instanceId);
            _logger.LogError("[{InstanceId}] ⚠️ Offset {Offset} for partition {Partition} was NOT committed", 
                _instanceId, result.Offset.Value, result.Partition.Value);
            _logger.LogError("[{InstanceId}] 🔄 This message WILL be redelivered if consumer restarts", _instanceId);
        }
    }
    
    private async Task ProcessDeadLetterQueue(CancellationToken cancellationToken)
    {
        var config = new ProducerConfig { BootstrapServers = _bootstrapServers };
        using var producer = new ProducerBuilder<string, string>(config).Build();
        
        while (!cancellationToken.IsCancellationRequested)
        {
            if (_deadLetterQueue.TryDequeue(out var item))
            {
                try
                {
                    var dlqMessage = new
                    {
                        OriginalOrder = item.order,
                        Error = item.error.Message,
                        Attempts = item.attempts,
                        ProcessedBy = _instanceId,
                        Timestamp = DateTime.UtcNow
                    };
                    
                    await producer.ProduceAsync(Topics.DeadLetters, new Message<string, string>
                    {
                        Key = item.order.OrderId,
                        Value = JsonSerializer.Serialize(dlqMessage)
                    });
                    
                    _logger.LogInformation("  [{InstanceId}] → DLQ: {OrderId}", _instanceId, item.order.OrderId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[{InstanceId}] DLQ failed", _instanceId);
                }
            }
            else
            {
                await Task.Delay(100, cancellationToken);
            }
        }
    }
    
    private class RetryInfo
    {
        public int Attempts { get; set; }
        public DateTime LastAttempt { get; set; } = DateTime.UtcNow;
    }
}