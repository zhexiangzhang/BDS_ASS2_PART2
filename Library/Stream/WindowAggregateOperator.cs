using Orleans.Streams;
using Utilities;
using SocialNetwork;
using System.Diagnostics;

namespace Library.Stream;

public interface IWindowAggregateOperator : IGrainWithStringKey
{
    // you should not change this interface
    Task Init(IAsyncStream<Event> inputStream, IAsyncStream<Event> outputStream, int windowSlide, int windowLength);
}

internal sealed class WindowAggregateOperator : Grain, IWindowAggregateOperator
{
    int windowSlide;
    int windowLength;
    IAsyncStream<Event> outputStream;

    long maxReceivedWatermark;

    // you can add more data structures here
    // ... ... ...

    public async Task Init(IAsyncStream<Event> inputStream, IAsyncStream<Event> outputStream, int windowSlide, int windowLength)
    {
        this.windowSlide = windowSlide;
        this.windowLength = windowLength;
        Debug.Assert(windowLength % windowSlide == 0);
        this.outputStream = outputStream;
        maxReceivedWatermark = Constants.initialWatermark;
        await inputStream.SubscribeAsync(ProcessEvent);
    }

    async Task ProcessEvent(Event e, StreamSequenceToken _)
    {
        switch (e.type)
        {
            case EventType.Regular:
                Debug.Assert(e.timestamp > maxReceivedWatermark);
                ProcessRegularEvent(e);
                break;
            case EventType.Watermark:
                maxReceivedWatermark = Math.Max(maxReceivedWatermark, e.timestamp);
                await ProcessWatermark(e.timestamp);
                break;
            default:
                throw new Exception($"Exception event type {e.type} is not supported. ");
        }
    }

    async Task ProcessWatermark(long timestamp)
    {
        throw new NotImplementedException();
    }

    async Task ProcessRegularEvent(Event e)
    {
        throw new NotImplementedException();
    }
}