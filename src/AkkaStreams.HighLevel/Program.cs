using System.Text.Json;
using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
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
        logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Information);
    })
    .ConfigureServices((context, services) =>
    {
        services.AddAkka("KafkaSystem", builder =>
        {
            // Configure Akka.NET logging to use Microsoft.Extensions.Logging
            builder.ConfigureLoggers(configBuilder =>
            {
                configBuilder.LogLevel = Akka.Event.LogLevel.InfoLevel;
                configBuilder.AddLoggerFactory();
            });
            
            // Use KafkaExtensions.DefaultSettings for cleaner configuration
            builder.AddHocon(KafkaExtensions.DefaultSettings, HoconAddMode.Append);
        });
    })
    .Build();

await host.StartAsync();

var logger = host.Services.GetRequiredService<ILogger<Program>>();
var system = host.Services.GetRequiredService<ActorSystem>();
var log = Logging.GetLogger(system, "AkkaStreamsHighLevel");
var materializer = system.Materializer();

// Simple bootstrap servers - either local or from environment variable
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
logger.LogInformation("Using Kafka at: {BootstrapServers}", bootstrapServers);

// Check if Kafka is available
if (!KafkaHelper.CheckKafkaAvailability(bootstrapServers))
{
    logger.LogError("Failed to connect to Kafka");
    await host.StopAsync();
    Environment.Exit(1);
}

logger.LogInformation("============================================");
logger.LogInformation("AKKA.STREAMS.KAFKA - INSTANCE {InstanceId}", instanceId);
logger.LogInformation("============================================");
logger.LogInformation("✅ ELEGANT ERROR HANDLING & REBALANCING ✅");
logger.LogInformation("Watch the simplicity of:");
logger.LogInformation("- Automatic retry with supervision");
logger.LogInformation("- Built-in dead letter handling");
logger.LogInformation("- Automatic backpressure");
logger.LogInformation("- Transparent offset management");
logger.LogInformation("- Seamless partition rebalancing");
logger.LogInformation("");
logger.LogInformation("💡 TIP: Run multiple instances to see smooth rebalancing!");
logger.LogInformation("   dotnet run 1  (first instance)");
logger.LogInformation("   dotnet run 2  (second instance)");
logger.LogInformation("   dotnet run 3  (third instance)");
logger.LogInformation("Press Ctrl+C to stop...");

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
var (control, completion) = KafkaConsumer
    .CommittableSource(consumerSettings, Subscriptions.Topics(Topics.Orders))
    .SelectAsync(10, async msg =>
    {
        try
        {
            var order = JsonSerializer.Deserialize<OrderEvent>(msg.Record.Message.Value);
            log.Info("[{0}:P{1}] Processing {2}", instanceId, msg.Record.Partition.Value, order!.OrderId);
            
            // Process with automatic retry via supervision
            var result = await processor.ProcessOrderAsync(order);
            log.Info("  [{0}] ✓ Processed {1}", instanceId, order.OrderId);
            
            return (Success: true, Message: msg, Order: order, Error: null as Exception);
        }
        catch (PoisonMessageException ex)
        {
            log.Warning("  [{0}] 🚫 POISON: {1}", instanceId, ex.Message);
            return (Success: false, Message: msg, Order: null as OrderEvent, Error: ex as Exception);
        }
        catch (ProcessingException ex) when (ex.IsTransient)
        {
            log.Warning("  [{0}] ⚠️ Transient error, will retry: {1}", instanceId, ex.Message);
            throw; // Let supervision strategy handle retry
        }
    })
    // Supervision strategy for automatic retry with backoff
    .WithAttributes(ActorAttributes.CreateSupervisionStrategy(ex =>
    {
        if (ex is ProcessingException { IsTransient: true })
        {
            log.Info("  [{0}] 🔄 Retrying after transient error", instanceId);
            return Akka.Streams.Supervision.Directive.Restart;
        }
        return Akka.Streams.Supervision.Directive.Stop;
    }))
    // Send failures to DLQ
    .SelectAsync(1, async result =>
    {
        if (result is not { Success: false, Order: not null }) return result.Message.CommitableOffset;
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
            
        log.Info("  [{0}] → DLQ: {1}", instanceId, result.Order.OrderId);

        return (ICommittable)result.Message.CommitableOffset;
    })
    // Commit offsets
    .ToMaterialized(
        Committer.Sink(committerSettings),
        Keep.Both)
    .Run(materializer);

// Handle rebalancing events elegantly
_ = control.IsShutdown.ContinueWith(t =>
{
    if (t.IsFaulted)
        logger.LogError(t.Exception, "[{InstanceId}] Stream failed", instanceId);
    else
        logger.LogInformation("[{InstanceId}] Stream completed gracefully", instanceId);
});

// Setup graceful shutdown
var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
var shutdownTcs = new TaskCompletionSource();

lifetime.ApplicationStopping.Register(() =>
{
    _ = ShutdownAsync(); // Fire and forget
    return;

    async Task ShutdownAsync()
    {
        logger.LogInformation("[{InstanceId}] Shutting down gracefully...", instanceId);
        try
        {
            await control.DrainAndShutdown(completion);
            logger.LogInformation("[{InstanceId}] Stream shutdown complete", instanceId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{InstanceId}] Error during stream shutdown", instanceId);
        }
        shutdownTcs.TrySetResult();
    }
});

// Wait for shutdown
await host.WaitForShutdownAsync();

logger.LogInformation("[{InstanceId}] ✓ Consumer stopped gracefully", instanceId);

// Reset terminal title
Console.Title = "";