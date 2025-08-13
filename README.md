# Akka.Streams.Kafka vs Confluent.Kafka Demo

This repository demonstrates the dramatic difference in complexity between using raw Confluent.Kafka drivers versus Akka.Streams.Kafka for building production-ready Kafka consumers in .NET.

## 🎯 Key Points Demonstrated

1. **Error Handling**: Retry logic, dead letter queues, poison message handling
2. **Partition Rebalancing**: How consumer groups handle scaling
3. **Backpressure**: Automatic flow control vs manual thread management
4. **Code Complexity**: ~350 lines vs ~50 lines for the same functionality

## 🚀 Quick Start

### Prerequisites
- .NET 9.0 SDK
- Docker & Docker Compose

### Setup Kafka

Start Kafka using Docker Compose:
```bash
docker-compose up -d
```

### Running the Demos

#### Option 1: Side-by-Side Comparison (Recommended)

Open four terminal windows:

**Terminal 1 - Generate Test Data:**
```bash
cd src/DataProducer
dotnet run
```

**Terminal 2 - Confluent.Kafka (Manual Approach):**
```bash
cd src/ConfluentKafka.LowLevel
dotnet run 1
```

**Terminal 3 - Akka.Streams.Kafka (Elegant Approach):**
```bash
cd src/AkkaStreams.HighLevel
dotnet run 1
```

Watch the difference in:
- Lines of code required
- Error handling complexity
- Rebalancing behavior

#### Option 2: Test Partition Rebalancing

**For Confluent.Kafka:**
```bash
# Terminal 1 - Start first instance (sets up Kafka)
cd src/ConfluentKafka.LowLevel
dotnet run 1

# Terminal 2 - Add second instance
cd src/ConfluentKafka.LowLevel
dotnet run 2

# Terminal 3 - Add third instance
cd src/ConfluentKafka.LowLevel
dotnet run 3
```

**For Akka.Streams.Kafka:**
```bash
# Terminal 1 - Start first instance (sets up Kafka)
cd src/AkkaStreams.HighLevel
dotnet run 1

# Terminal 2 - Add second instance
cd src/AkkaStreams.HighLevel
dotnet run 2

# Terminal 3 - Add third instance
cd src/AkkaStreams.HighLevel
dotnet run 3
```

## 📊 What to Observe

### In Confluent.Kafka Implementation:
- **350+ lines** of complex error handling code
- Manual retry logic with exponential backoff
- Thread pool management and synchronization
- Complex partition rebalancing callbacks
- Manual offset management with locking
- Separate dead letter queue processing

### In Akka.Streams.Kafka Implementation:
- **~50 lines** total for the same functionality
- Automatic retry via supervision strategies
- Built-in backpressure and flow control
- Transparent partition rebalancing
- Automatic offset management
- Integrated dead letter handling

## 🔍 Test Scenarios

The demo automatically generates 100 orders with:
- **Normal orders**: Process successfully
- **Transient failures**: Fail first 2 attempts, succeed on 3rd
- **Poison messages**: Invalid data that goes straight to DLQ
- **Random failures**: 10% chance of transient failure
- **Large orders**: Require additional processing time

## 📈 Key Metrics

| Aspect | Confluent.Kafka | Akka.Streams.Kafka |
|--------|-----------------|-------------------|
| Lines of Code | ~350 | ~50 |
| Error Handling | Manual try-catch everywhere | Supervision strategies |
| Retry Logic | Manual implementation | Built-in with backoff |
| Backpressure | Manual semaphores | Automatic |
| Rebalancing | Complex callbacks | Transparent |
| Thread Safety | Manual locking | Actor model |
| Dead Letters | Separate queue processing | Integrated flow |

## 🎥 Video Script Points

1. **The Hook**: "You're writing 100s of lines for basic Kafka error handling"
2. **The Problem**: Show the Confluent.Kafka complexity
3. **The Solution**: Show the same thing in Akka.Streams.Kafka
4. **The Proof**: Run both and show identical behavior
5. **The Scaling Test**: Add instances and watch rebalancing
6. **The Conclusion**: "Why write plumbing when you can write business logic?"

## 🛠️ Architecture

```
┌─────────────────┐
│  Kafka Topic    │
│    (orders)     │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
┌────▼──┐ ┌───▼───┐
│ Manual│ │Streams│
│Consumer│ │Consumer│
└────┬──┘ └───┬───┘
     │        │
┌────▼────────▼───┐
│  Order Processor │
│  (Shared Logic)  │
└────────┬─────────┘
         │
    ┌────▼────┐
    │   DLQ   │
    └─────────┘
```

## 📝 Notes

- Kafka is automatically started using Testcontainers
- First instance (run with `1`) sets up Kafka and generates data
- Additional instances (run with `2`, `3`, etc.) connect to the same Kafka
- Press Ctrl+C to gracefully shutdown

## 🔗 Resources

- [Akka.Streams.Kafka Documentation](https://github.com/akkadotnet/Akka.Streams.Kafka)
- [Confluent.Kafka Documentation](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html)
- [Video: The Best Kafka Client in .NET](#) (Coming Soon)
