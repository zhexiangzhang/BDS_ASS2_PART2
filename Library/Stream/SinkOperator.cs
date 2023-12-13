using Orleans.Streams;
using Utilities;
using SocialNetwork;

namespace Library.Stream;

public interface ISinkOperator : IGrainWithStringKey
{
    Task Init(IAsyncStream<Event> inputStream, string resultFile);
}

internal sealed class SinkOperator : Grain, ISinkOperator
{
    string resultFile;

    public async Task Init(IAsyncStream<Event> inputStream, string resultFile)
    {
        this.resultFile = resultFile;

        // whenever the operator receives an event, the method "ProcessEvent" is called automatically
        await inputStream.SubscribeAsync(ProcessEvent);
    }

    Task ProcessEvent(Event e, StreamSequenceToken _)
    {
        Functions.Sink(resultFile, e);
        return Task.CompletedTask;
    }
}
