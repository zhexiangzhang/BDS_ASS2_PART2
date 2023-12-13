using SocialNetwork;
using System.Diagnostics;
using Orleans.Streams;
using Utilities;

namespace Library.Stream;

public interface IFilterOperator : IGrainWithStringKey
{
    Task Init(IAsyncStream<Event> inputStream, IAsyncStream<Event> outputStream);
}

internal sealed class FilterOperator : Grain, IFilterOperator
{
    long maxReceivedWatermark;
    IAsyncStream<Event> outputStream;
    
    public async Task Init(IAsyncStream<Event> inputStream, IAsyncStream<Event> outputStream)
    {
        maxReceivedWatermark = Constants.initialWatermark;
        this.outputStream = outputStream;

        // whenever the operator receives an event, the method "ProcessEvent" is called automatically
        await inputStream.SubscribeAsync(ProcessEvent);
    }

    async Task ProcessEvent(Event e, StreamSequenceToken _)
    {
        switch (e.type)
        {
            case EventType.Regular:
                Debug.Assert(e.timestamp > maxReceivedWatermark);
                if (Functions.Filter(e)) await outputStream.OnNextAsync(e);
                break;
            case EventType.Watermark:
                maxReceivedWatermark = Math.Max(maxReceivedWatermark, e.timestamp);
                await outputStream.OnNextAsync(e);
                break;
            default:
                throw new Exception($"Exception event type {e.type} is not supported. ");
        }
    }
}