using Orleans.Streams;
using Utilities;
using SocialNetwork;
using System.Diagnostics;

namespace Library.Stream;

public interface IWindowJoinOperator : IGrainWithStringKey
{
    // you should not change this interface
    Task Init(IAsyncStream<Event> inputStream1, IAsyncStream<Event> inputStream2, IAsyncStream<Event> outputStream, int windowSlide, int windowLength);
}

internal sealed class WindowJoinOperator : Grain, IWindowJoinOperator
{
    int windowSlide;
    int windowLength;
    IAsyncStream<Event> outputStream;

    long[] maxReceivedWatermark;

    // you can add more data structures here
    // ... ... ...

    public async Task Init(IAsyncStream<Event> inputStream1, IAsyncStream<Event> inputStream2, IAsyncStream<Event> outputStream, int windowSlide, int windowLength)
    {
        this.windowSlide = windowSlide;
        this.windowLength = windowLength;
        Debug.Assert(windowLength % windowSlide == 0);
        this.outputStream = outputStream;
        
        maxReceivedWatermark = new long[2] { Constants.initialWatermark, Constants.initialWatermark };

        // whenever the operator receives an event, the method "ProcessEvent" is called automatically
        await inputStream1.SubscribeAsync(ProcessEvent1);
        await inputStream2.SubscribeAsync(ProcessEvent2);
    }

    async Task ProcessEvent1(Event e, StreamSequenceToken _)
    {
        switch (e.type)
        {
            case EventType.Regular:
                Debug.Assert(e.timestamp > maxReceivedWatermark[0]);
                ProcessRegularEvent(e, 1);
                break;
            case EventType.Watermark:
                maxReceivedWatermark[0] = Math.Max(maxReceivedWatermark[0], e.timestamp);
                await ProcessWatermark();
                break;
            default:
                throw new Exception($"Exception event type {e.type} is not supported. ");
        }
    }

    async Task ProcessEvent2(Event e, StreamSequenceToken _)
    {
        switch (e.type)
        {
            case EventType.Regular:
                Debug.Assert(e.timestamp > maxReceivedWatermark[1]);
                ProcessRegularEvent(e, 2);
                break;
            case EventType.Watermark:
                maxReceivedWatermark[1] = Math.Max(maxReceivedWatermark[1], e.timestamp);
                await ProcessWatermark();
                break;
            default:
                throw new Exception($"Exception event type {e.type} is not supported. ");
        }
    }

    async Task ProcessWatermark()
    {
        throw new NotImplementedException();
    }

    async Task ProcessRegularEvent(Event e, int sourceID)
    {
        throw new NotImplementedException();
    }
}