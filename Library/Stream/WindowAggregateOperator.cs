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
    
    // For aggregation, slide and length are the same, so there are no overlapping, each event belongs to a single window
    // THE key represents the windowID (start timestamp of the window) eg: 3 to represent [3, 9)
    // eg [3, 6) , buffer=<3,A,A> , when watermark <4> coming, its not the time to send the window, as event <5> can still come
    // to guarantee all events in the window are received, check watermark > maxTimeStamp in the window = windowID + windowLength - 1
    SortedDictionary<long, List<Event>> streamBuffer = new SortedDictionary<long, List<Event>>();    

    public async Task Init(IAsyncStream<Event> inputStream, IAsyncStream<Event> outputStream, int windowSlide, int windowLength)
    {
        this.windowSlide = windowSlide;
        this.windowLength = windowLength;
        Debug.Assert(windowLength % windowSlide == 0);
        this.outputStream = outputStream;
        maxReceivedWatermark = Constants.initialWatermark;

        this.streamBuffer = new SortedDictionary<long, List<Event>>();

        await inputStream.SubscribeAsync(ProcessEvent);
    }

    async Task ProcessEvent(Event e, StreamSequenceToken _)
    {
        switch (e.type)
        {
            case EventType.Regular:
                Debug.Assert(e.timestamp > maxReceivedWatermark);
                await ProcessRegularEvent(e);
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
        // The aggregation result should be done on a per-window basis
        // Process aggregation only the current watermark can determine the entire window to be sent. 
        // Should not compare the watermark with the timestamp of a specific event.
        
        // if maxReceivedWatermark > maxTimeStamp in the window = windowID + windowLength - 1, then we can aggregate the window
        var windowsIDs = streamBuffer.Keys;
        foreach (long id in windowsIDs)
        {
            // Console.WriteLine($"****************");
            // Console.WriteLine($"windowID = {id}");
            long maxTimeStampInWindow = id + windowLength - 1;

            if(maxReceivedWatermark > maxTimeStampInWindow)
            {
                //pass on the list of events in the window to the aggregate function
                long newTimestamp = maxTimeStampInWindow;
                List<Event> aggregated_result = Functions.WindowAggregator(newTimestamp, streamBuffer[id]);                
                //send the window to the output stream
                foreach (Event e in aggregated_result){                    
                    await outputStream.OnNextAsync(e);
                }                
                //remove the window from the dictionary
                streamBuffer.Remove(id);
            }
            else
            {
                break; // because the events are sorted by timestamp                
            }
        }
    }

    async Task ProcessRegularEvent(Event e)
    {        
        long windowID = getWindowInstances(e.timestamp, windowSlide, windowLength);
        // insert into the buffer dictionary
        if (!streamBuffer.ContainsKey(windowID))
        {            
            streamBuffer.Add(windowID, new List<Event>());            
        }        
        streamBuffer[windowID].Add(e);        
    }

    // for a single event, find the window instance that it belongs to
    public static long getWindowInstances(long eventTimestamp, int windowSlide, int windowLength)
    {                                        
        // the offset of the event from the start of the window, eg : offset = 4 % 3 = 1
        long offset = eventTimestamp % windowSlide;
        // Note negative value, eg : eventTimestamp = -2 in [-3, 3) , offset = -2 % 3 = -2
        if (eventTimestamp < 0) offset = windowSlide + offset;

        // use startWindow to represent the window instance id, eg 3 to represent [3, 9)
        long windowID = eventTimestamp - offset; // eg : 4 - 1 = 3, means 4 is in [3, 9)
    
        return windowID;
    }
}