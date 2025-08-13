using System.Text.Json;
using Akka;
using Akka.Actor;
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

// Setup Kafka with Testcontainers
var kafkaSetup = new KafkaSetup();
await kafkaSetup.StartAsync();
var bootstrapServers = kafkaSetup.BootstrapServers;

// Save bootstrap servers for consumers
await File.WriteAllTextAsync("kafka.txt", bootstrapServers);

Console.WriteLine($"Kafka started at: {bootstrapServers}");

// Create actor system
var system = ActorSystem.Create("ProducerSystem");

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
Console.WriteLine($"   - Normal orders: {orders.Count(o => !o.OrderId.StartsWith("POISON") && !o.OrderId.StartsWith("TRANSIENT"))}");
Console.WriteLine($"   - Poison messages: {orders.Count(o => o.OrderId.StartsWith("POISON"))}");
Console.WriteLine($"   - Transient failures: {orders.Count(o => o.OrderId.StartsWith("TRANSIENT"))}");

Console.WriteLine("\n✓ Producer completed. Kafka is running in the background.");
Console.WriteLine("  Consumers can now connect using the bootstrap servers in kafka.txt");
Console.WriteLine("\nPress Ctrl+C to stop Kafka and exit...");

Console.CancelKeyPress += async (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine("\nShutting down...");
    await system.Terminate();
    await kafkaSetup.DisposeAsync();
    File.Delete("kafka.txt");
    Environment.Exit(0);
};

// Keep running to keep Kafka alive
await Task.Delay(Timeout.Infinite);
