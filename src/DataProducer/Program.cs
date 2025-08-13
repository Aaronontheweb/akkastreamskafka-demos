using System.Text.Json;
using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Shared;

// Setup host with logging
var host = new HostBuilder()
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
        logging.SetMinimumLevel(LogLevel.Information);
    })
    .ConfigureServices((context, services) =>
    {
        services.AddAkka("ProducerSystem", builder =>
        {
            builder.ConfigureLoggers(configBuilder =>
            {
                configBuilder.LogLevel = Akka.Event.LogLevel.InfoLevel;
                configBuilder.AddLoggerFactory();
            });
            
            builder.AddHocon(ConfigurationFactory.ParseString(@"
                akka.kafka.producer {
                    kafka-clients {
                        bootstrap.servers = """"
                    }
                }
            ").WithFallback(
                ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf")),
                HoconAddMode.Append);
        });
    })
    .Build();

await host.StartAsync();

var logger = host.Services.GetRequiredService<ILogger<Program>>();
var system = host.Services.GetRequiredService<ActorSystem>();
var log = Akka.Event.Logging.GetLogger(system, "DataProducer");

logger.LogInformation("===========================================");
logger.LogInformation("AKKA.STREAMS.KAFKA PRODUCER");
logger.LogInformation("===========================================");

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

// Configure producer settings
var producerSettings = ProducerSettings<string, string>
    .Create(system, Serializers.Utf8, Serializers.Utf8)
    .WithBootstrapServers(bootstrapServers);

// Create data generator
var generator = new DemoDataGenerator();

// Setup cancellation using host lifetime
var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
var cts = CancellationTokenSource.CreateLinkedTokenSource(lifetime.ApplicationStopping);

// Run producer indefinitely with bursts every 30 seconds
var burstNumber = 0;
var burstSize = 50; // Generate 50 orders per burst
var burstInterval = TimeSpan.FromSeconds(30);

logger.LogInformation("Starting continuous producer - generating {BurstSize} orders every {Interval} seconds", 
    burstSize, burstInterval.TotalSeconds);
logger.LogInformation("Press Ctrl+C to stop...");

while (!cts.Token.IsCancellationRequested)
{
    burstNumber++;
    var orders = generator.GenerateOrders(burstSize).ToList();
    
    logger.LogInformation("📤 Burst #{BurstNumber}: Producing {Count} orders to topic '{Topic}'...", 
        burstNumber, orders.Count, Topics.Orders);
    
    var startTime = DateTime.UtcNow;
    
    try
    {
        await Source.From(orders)
            .Select(order => new ProducerRecord<string, string>(
                Topics.Orders,
                order.OrderId,
                JsonSerializer.Serialize(order)))
            .RunWith(KafkaProducer.PlainSink(producerSettings), system.Materializer())
            .ConfigureAwait(false);
        
        var elapsed = DateTime.UtcNow - startTime;
        
        logger.LogInformation("✅ Burst #{BurstNumber} completed: {Count} orders in {Elapsed:F2}s ({Rate:F0} orders/sec)",
            burstNumber, orders.Count, elapsed.TotalSeconds, orders.Count / elapsed.TotalSeconds);
        
        // Log order types
        var normalCount = orders.Count(o => !o.OrderId.StartsWith("POISON") && !o.OrderId.StartsWith("TRANSIENT"));
        var poisonCount = orders.Count(o => o.OrderId.StartsWith("POISON"));
        var transientCount = orders.Count(o => o.OrderId.StartsWith("TRANSIENT"));
        
        if (poisonCount > 0 || transientCount > 0)
        {
            logger.LogInformation("   Order types: Normal={Normal}, Poison={Poison}, Transient={Transient}",
                normalCount, poisonCount, transientCount);
        }
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Failed to produce burst #{BurstNumber}", burstNumber);
    }
    
    // Wait for next burst (check for cancellation every second)
    logger.LogInformation("⏱️ Next burst in {Seconds} seconds...", burstInterval.TotalSeconds);
    
    for (int i = 0; i < burstInterval.TotalSeconds && !cts.Token.IsCancellationRequested; i++)
    {
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token).ConfigureAwait(false);
    }
}

logger.LogInformation("Producer stopped");
await host.StopAsync();