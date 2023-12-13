using Orleans.Streams;
using Utilities;
using Confluent.Kafka;

namespace Library.Stream;

public interface IKafkaConnector : IGrainWithStringKey
{
    Task Init(IAsyncStream<Event> inputStream, string outputTopic);
}

internal sealed class KafkaConnector : Grain, IKafkaConnector
{
    string outputTopic;
    IProducer<Null, Event> kafkaProducer;

    public async Task Init(IAsyncStream<Event> inputStream, string outputTopic)
    {
        // set up the output channel to kafka
        this.outputTopic = outputTopic;
        var config = new ProducerConfig { BootstrapServers = Constants.kafkaService };
        var kafkaBuilder = new ProducerBuilder<Null, Event>(config).SetValueSerializer(new EventSerializer());
        kafkaProducer = kafkaBuilder.Build();

        await inputStream.SubscribeAsync(ProcessEvent);
    }

    async Task ProcessEvent(Event e, StreamSequenceToken _)
    {
        // output the event to kafka (external service)
        // here we use the .NET kafka client implemented by Confluent
        await kafkaProducer.ProduceAsync(outputTopic, new Message<Null, Event>
        {
            Timestamp = new Timestamp(e.timestamp, TimestampType.CreateTime),
            Value = e
        });
    }
}