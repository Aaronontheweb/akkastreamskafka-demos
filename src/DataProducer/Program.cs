using System.Text.Json;
using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Shared;

Console.WriteLine("===========================================");
Console.WriteLine("AKKA.STREAMS.KAFKA PRODUCER");
Console.WriteLine("===========================================");

// Simple bootstrap servers - either local or from environment variable
var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
Console.WriteLine($"Using Kafka at: {bootstrapServers}");

// Check if Kafka is available
if (!KafkaHelper.CheckKafkaAvailability(bootstrapServers))
{
    Environment.Exit(1);
}

// Create actor system with proper Kafka configuration
var config = ConfigurationFactory.ParseString(@"
    akka.kafka.producer {
        kafka-clients {
            bootstrap.servers = """"
        }
    }
")
    .WithFallback(
        ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

var system = ActorSystem.Create("ProducerSystem", config);

// Configure producer settings
var producerSettings = ProducerSettings<string, string>
    .Create(system, Serializers.Utf8, Serializers.Utf8)
    .WithBootstrapServers(bootstrapServers);

// Generate and produce orders
var generator = new DemoDataGenerator();
var orders = generator.GenerateOrders(100).ToList();

Console.WriteLine($"\nProducing {orders.Count} orders to topic '{Topics.Orders}'...");

var startTime = DateTime.UtcNow;

await Source.From(orders)
    .Select(order => new ProducerRecord<string, string>(
        Topics.Orders,
        order.OrderId,
        JsonSerializer.Serialize(order)))
    .Batch(10, seed => new List<ProducerRecord<string, string>> { seed }, (list, item) =>
    {
        list.Add(item);
        return list;
    })
    .Select(batch =>
    {
        Console.WriteLine($"  ✓ Produced batch of {batch.Count} orders");
        return batch;
    })
    .SelectMany(batch => batch)
    .RunWith(KafkaProducer.PlainSink(producerSettings), system.Materializer());

var elapsed = DateTime.UtcNow - startTime;
Console.WriteLine($"\n✅ Successfully produced {orders.Count} orders in {elapsed.TotalSeconds:F2}s");
Console.WriteLine($"   Average: {orders.Count / elapsed.TotalSeconds:F0} orders/sec");

Console.WriteLine("\n📊 Order Types Generated:");
Console.WriteLine(
    $"   - Normal orders: {orders.Count(o => !o.OrderId.StartsWith("POISON") && !o.OrderId.StartsWith("TRANSIENT"))}");
Console.WriteLine($"   - Poison messages: {orders.Count(o => o.OrderId.StartsWith("POISON"))}");
Console.WriteLine($"   - Transient failures: {orders.Count(o => o.OrderId.StartsWith("TRANSIENT"))}");

Console.WriteLine("\n✓ Producer completed.");

await system.Terminate();