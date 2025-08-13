using Confluent.Kafka;

namespace Shared;

public static class KafkaHelper
{
    public static bool CheckKafkaAvailability(string bootstrapServers)
    {
        Console.WriteLine("Checking Kafka availability...");
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
        
        using var adminClient = new AdminClientBuilder(adminConfig).Build();
        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            Console.WriteLine($"✓ Connected to Kafka cluster with {metadata.Brokers.Count} broker(s)");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Failed to connect to Kafka at {bootstrapServers}");
            Console.WriteLine($"   Error: {ex.Message}");
            Console.WriteLine("\nPlease ensure Kafka is running:");
            Console.WriteLine("   docker-compose up -d");
            return false;
        }
    }
}