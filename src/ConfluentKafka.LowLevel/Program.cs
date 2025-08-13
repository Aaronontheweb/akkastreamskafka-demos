using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Shared;

// Parse command line args
var instanceId = args.Length > 0 ? args[0] : "1";
Console.Title = $"Confluent.Kafka Instance {instanceId}";

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

// Wait for producer to set up Kafka
string bootstrapServers;
while (!File.Exists("kafka.txt"))
{
    Console.WriteLine("Waiting for producer to start Kafka...");
    Console.WriteLine("Please run the DataProducer first: dotnet run --project src/DataProducer");
    await Task.Delay(2000);
}
bootstrapServers = await File.ReadAllTextAsync("kafka.txt");

Console.WriteLine("\n============================================");
Console.WriteLine($"CONFLUENT.KAFKA - INSTANCE {instanceId}");
Console.WriteLine("============================================");
Console.WriteLine("\n🔴 MANUAL ERROR HANDLING & REBALANCING 🔴\n");
Console.WriteLine("Watch the complexity of:");
Console.WriteLine("- Retry logic with exponential backoff");
Console.WriteLine("- Dead letter queue management");
Console.WriteLine("- Thread pool coordination");
Console.WriteLine("- Manual offset management");
Console.WriteLine("- Partition rebalancing callbacks");
Console.WriteLine("\n💡 TIP: Run multiple instances to see rebalancing chaos!");
Console.WriteLine("   dotnet run 1  (first instance)");
Console.WriteLine("   dotnet run 2  (second instance)");
Console.WriteLine("   dotnet run 3  (third instance)");
Console.WriteLine("\nPress Ctrl+C to stop...\n");

// Start the complex consumer
var consumer = new ManualErrorHandlingConsumer(bootstrapServers, instanceId);
await consumer.StartAsync(cts.Token);

Console.WriteLine("\n✓ Consumer stopped gracefully");

public class ManualErrorHandlingConsumer
{
    private readonly string _bootstrapServers;
    private readonly string _instanceId;
    private readonly OrderProcessor _processor = new();
    private readonly ConcurrentDictionary<string, RetryInfo> _retryTracker = new();
    private readonly ConcurrentQueue<(OrderEvent order, int attempts, Exception error)> _deadLetterQueue = new();
    private readonly SemaphoreSlim _processingThrottle = new(10);
    private readonly object _offsetLock = new();
    private readonly Dictionary<TopicPartition, Offset> _pendingOffsets = new();
    private readonly HashSet<TopicPartition> _assignedPartitions = new();
    
    public ManualErrorHandlingConsumer(string bootstrapServers, string instanceId)
    {
        _bootstrapServers = bootstrapServers;
        _instanceId = instanceId;
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
                Console.WriteLine($"\n[REBALANCE-{_instanceId}] ASSIGNED: {string.Join(", ", partitions.Select(p => $"P{p.Partition.Value}"))}");
                
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
                Console.WriteLine($"\n[REBALANCE-{_instanceId}] REVOKED: {string.Join(", ", partitions.Select(p => $"P{p.Partition.Value}"))}");
                
                // Try to commit any pending offsets before revocation
                try
                {
                    c.Commit();
                }
                catch (KafkaException ex)
                {
                    Console.WriteLine($"[ERROR] Failed to commit during revocation: {ex.Message}");
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
                Console.WriteLine($"\n[REBALANCE-{_instanceId}] LOST: {string.Join(", ", partitions.Select(p => $"P{p.Partition.Value}"))}");
                
                // Partitions were lost without proper revocation - potential data loss!
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
                Console.WriteLine($"[ERROR-{_instanceId}] {error.Reason}");
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
                            Console.WriteLine($"[WARN-{_instanceId}] Received message from unassigned partition {consumeResult.TopicPartition}");
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
                        Console.WriteLine($"[ERROR-{_instanceId}] Failed to deserialize: {ex.Message}");
                        consumer.Commit(consumeResult);
                        continue;
                    }
                    
                    Console.WriteLine($"[{_instanceId}:P{consumeResult.Partition.Value}] Processing {order.OrderId}");
                    
                    // Process with complex error handling
                    await _processingThrottle.WaitAsync(cancellationToken);
                    
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
                    Console.WriteLine($"[ERROR-{_instanceId}] Consume error: {ex.Error.Reason}");
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
                    Console.WriteLine($"  [{_instanceId}] Retry {attempt} for {order.OrderId} after {delay.TotalSeconds}s");
                    await Task.Delay(delay);
                }
                
                var result = await _processor.ProcessOrderAsync(order);
                Console.WriteLine($"  [{_instanceId}] ✓ Processed {order.OrderId}");
                
                // Manual offset management with locking
                lock (_offsetLock)
                {
                    var tp = new TopicPartition(consumeResult.Topic, consumeResult.Partition);
                    _pendingOffsets[tp] = consumeResult.Offset + 1;
                    
                    try
                    {
                        consumer.Commit(_pendingOffsets.Select(kv => 
                            new TopicPartitionOffset(kv.Key, kv.Value)));
                    }
                    catch (KafkaException ex)
                    {
                        Console.WriteLine($"[ERROR-{_instanceId}] Commit failed: {ex.Message}");
                    }
                }
                
                _retryTracker.TryRemove(order.OrderId, out _);
                return;
            }
            catch (PoisonMessageException ex)
            {
                Console.WriteLine($"  [{_instanceId}] 🚫 POISON: {order.OrderId}");
                _deadLetterQueue.Enqueue((order, attempt, ex));
                CommitOffsetSafely(consumer, consumeResult);
                return;
            }
            catch (ProcessingException ex) when (ex.IsTransient && attempt < 3)
            {
                Console.WriteLine($"  [{_instanceId}] ⚠️ Transient failure attempt {attempt}");
                continue;
            }
            catch (Exception ex)
            {
                if (attempt == 3)
                {
                    Console.WriteLine($"  [{_instanceId}] ❌ Failed after 3 attempts");
                    _deadLetterQueue.Enqueue((order, attempt, ex));
                    CommitOffsetSafely(consumer, consumeResult);
                }
            }
        }
    }
    
    private void CommitOffsetSafely(IConsumer<string, string> consumer, ConsumeResult<string, string> result)
    {
        try
        {
            consumer.Commit(result);
        }
        catch (KafkaException ex)
        {
            Console.WriteLine($"[ERROR-{_instanceId}] Commit failed: {ex.Message}");
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
                    
                    Console.WriteLine($"  [{_instanceId}] → DLQ: {item.order.OrderId}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR-{_instanceId}] DLQ failed: {ex.Message}");
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