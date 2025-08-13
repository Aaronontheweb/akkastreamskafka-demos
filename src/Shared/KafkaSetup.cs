using Testcontainers.Kafka;

namespace Shared;

public class KafkaSetup : IAsyncDisposable
{
    private KafkaContainer? _container;
    
    public string BootstrapServers { get; private set; } = "";
    
    public async Task StartAsync()
    {
        Console.WriteLine("Starting Kafka container...");
        
        _container = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.5.0")
            .Build();
            
        await _container.StartAsync();
        
        BootstrapServers = _container.GetBootstrapAddress();
        Console.WriteLine($"Kafka started at: {BootstrapServers}");
    }
    
    public async ValueTask DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
    }
}