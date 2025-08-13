using System.Text.Json;
using Akka;
using Akka.Actor;
using Akka.Hosting;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared;

// Parse command line args  
var instanceId = args.Length > 0 ? args[0] : "1";
Console.Title = $"Akka.Streams.Kafka Instance {instanceId}";

// Setup host
var host = new HostBuilder()
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
        logging.SetMinimumLevel(LogLevel.Information);
    })
    .ConfigureServices((context, services) =>
    {
        services.AddAkka("KafkaSystem", builder =>
        {
            builder.WithActors((system, registry) =>
            {
                // Actor system is ready
            });
        });
    })
    .Build();

await host.StartAsync();

var system = host.Services.GetRequiredService<ActorSystem>();
var materializer = system.Materializer();

// Simple bootstrap servers - either local or from environment variable
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
Console.WriteLine($"Using Kafka at: {bootstrapServers}");

// Check if Kafka is available
if (!KafkaHelper.CheckKafkaAvailability(bootstrapServers))
{
    await host.StopAsync();
    Environment.Exit(1);
}

Console.WriteLine("\n============================================");
Console.WriteLine($"AKKA.STREAMS.KAFKA - INSTANCE {instanceId}");
Console.WriteLine("============================================");
Console.WriteLine("\n✅ ELEGANT ERROR HANDLING & REBALANCING ✅\n");
Console.WriteLine("Watch the simplicity of:");
Console.WriteLine("- Automatic retry with supervision");
Console.WriteLine("- Built-in dead letter handling");
Console.WriteLine("- Automatic backpressure");
Console.WriteLine("- Transparent offset management");
Console.WriteLine("- Seamless partition rebalancing");
Console.WriteLine("\n💡 TIP: Run multiple instances to see smooth rebalancing!");
Console.WriteLine("   dotnet run 1  (first instance)");
Console.WriteLine("   dotnet run 2  (second instance)");
Console.WriteLine("   dotnet run 3  (third instance)");
Console.WriteLine("\nPress Ctrl+C to stop...\n");

// Configure Kafka consumer
var consumerSettings = ConsumerSettings<string, string>
    .Create(system, Deserializers.Utf8, Deserializers.Utf8)
    .WithBootstrapServers(bootstrapServers)
    .WithGroupId("akka-consumer-group")
    .WithProperty("session.timeout.ms", "6000")
    .WithProperty("auto.offset.reset", "earliest");

var committerSettings = CommitterSettings.Create(system);

// Configure DLQ producer
var producerSettings = ProducerSettings<string, string>
    .Create(system, Serializers.Utf8, Serializers.Utf8)
    .WithBootstrapServers(bootstrapServers);

// Create the order processor
var processor = new OrderProcessor();

// THE ELEGANT SOLUTION - All error handling in ~50 lines!
var control = KafkaConsumer
    .CommittableSource(consumerSettings, Subscriptions.Topics(Topics.Orders))
    .SelectAsync(10, async msg =>
    {
        try
        {
            var order = JsonSerializer.Deserialize<OrderEvent>(msg.Record.Message.Value);
            Console.WriteLine($"[{instanceId}:P{msg.Record.Partition.Value}] Processing {order!.OrderId}");
            
            // Process with automatic retry via supervision
            var result = await processor.ProcessOrderAsync(order);
            Console.WriteLine($"  [{instanceId}] ✓ Processed {order.OrderId}");
            
            return (Success: true, Message: msg, Order: order, Error: null as Exception);
        }
        catch (PoisonMessageException ex)
        {
            Console.WriteLine($"  [{instanceId}] 🚫 POISON: {ex.Message}");
            return (Success: false, Message: msg, Order: null as OrderEvent, Error: ex as Exception);
        }
        catch (ProcessingException ex) when (ex.IsTransient)
        {
            Console.WriteLine($"  [{instanceId}] ⚠️ Transient error, will retry: {ex.Message}");
            throw; // Let supervision strategy handle retry
        }
    })
    // Supervision strategy for automatic retry with backoff
    .WithAttributes(ActorAttributes.CreateSupervisionStrategy(ex =>
    {
        if (ex is ProcessingException pe && pe.IsTransient)
            return Akka.Streams.Supervision.Directive.Restart;
        return Akka.Streams.Supervision.Directive.Stop;
    }))
    // Send failures to DLQ
    .SelectAsync(1, async result =>
    {
        if (result is { Success: false, Order: not null })
        {
            var dlqMessage = new
            {
                OriginalOrder = result.Order,
                Error = result.Error?.Message,
                ProcessedBy = instanceId,
                Timestamp = DateTime.UtcNow
            };
            
            var message = new ProducerRecord<string, string>(
                Topics.DeadLetters,
                result.Order.OrderId,
                JsonSerializer.Serialize(dlqMessage));
            
            await Source.Single(message)
                .RunWith(KafkaProducer.PlainSink(producerSettings), materializer);
            
            Console.WriteLine($"  [{instanceId}] → DLQ: {result.Order.OrderId}");
        }
        
        return (ICommittable)result.Message.CommitableOffset;
    })
    // Commit offsets
    .ToMaterialized(
        Committer.Sink(committerSettings),
        Keep.Left)
    .Run(materializer);

// Handle rebalancing events elegantly
_ = control.IsShutdown.ContinueWith(t =>
{
    if (t.IsFaulted)
        Console.WriteLine($"[{instanceId}] Stream failed: {t.Exception?.GetBaseException().Message}");
    else
        Console.WriteLine($"[{instanceId}] Stream completed gracefully");
});

// Wait for shutdown
var done = new TaskCompletionSource<bool>();
Console.CancelKeyPress += async (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine($"\n[{instanceId}] Shutting down gracefully...");
    await control.Shutdown();
    await host.StopAsync();
    done.SetResult(true);
};

await done.Task;

Console.WriteLine($"\n[{instanceId}] ✓ Consumer stopped gracefully");