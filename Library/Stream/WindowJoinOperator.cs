using Orleans.Streams;
using Utilities;
using SocialNetwork;
using System.Diagnostics;
using System.Reflection.Metadata;

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
    long lastAlignmentWM = Constants.initialWatermark;
    List<Event> tagStreamBuffer;
    List<Event> likeStreamBuffer;

    public async Task Init(IAsyncStream<Event> inputStream1, IAsyncStream<Event> inputStream2, IAsyncStream<Event> outputStream, int windowSlide, int windowLength)
    {
        this.windowSlide = windowSlide;
        this.windowLength = windowLength;
        Debug.Assert(windowLength % windowSlide == 0);
        this.outputStream = outputStream;
        maxReceivedWatermark = new long[2] { Constants.initialWatermark, Constants.initialWatermark };

        tagStreamBuffer = new List<Event>();
        likeStreamBuffer = new List<Event>();

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
                await ProcessRegularEvent(e, 1);
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
                await ProcessRegularEvent(e, 2);
                break;
            case EventType.Watermark:
                maxReceivedWatermark[1] = Math.Max(maxReceivedWatermark[1], e.timestamp);
                await ProcessWatermark();
                break;
            default:
                throw new Exception($"Exception event type {e.type} is not supported. ");
        }
    }

    // TODO: Can be optimized: use sorted list to store the events, so that we can prune the events in O(logn) time
    async Task ProcessWatermark()
    {
        long maxWMInTagStream = maxReceivedWatermark[0];
        long maxWMInLikeStream = maxReceivedWatermark[1];
        
        // Console.WriteLine($"[New WaterMark] {maxWMInTagStream}, {maxWMInLikeStream}");

        for (int i=0; i < tagStreamBuffer.Count; i++){
            var e = tagStreamBuffer[i];
            // use maxWMInLikeStream to prune the tagStreamBuffer
            if(e.timestamp < maxWMInLikeStream - windowLength) tagStreamBuffer.RemoveAt(i);            
        }        
    
        for (int i=0; i < likeStreamBuffer.Count; i++){
            var e = likeStreamBuffer[i];
            // use maxWMInTagStream to prune the likeStreamBuffer
            if(e.timestamp < maxWMInTagStream - windowLength) likeStreamBuffer.RemoveAt(i);            
        }

        long alignmentWM = Math.Min(maxWMInTagStream, maxWMInLikeStream);

        if (alignmentWM > lastAlignmentWM) {            
            lastAlignmentWM = alignmentWM;
            // send the watermark to the downstream
            // FIXME: confirm watermark timestamp ??
            // Console.WriteLine($" ****** send WaterMark ******* {lastAlignmentWM}");
            var watermark = Event.CreateEvent(lastAlignmentWM, EventType.Watermark, new byte[0]);
            await outputStream.OnNextAsync(watermark);
        }
    }

    async Task ProcessRegularEvent(Event e, int sourceID)
    {
        // for default, sourceID = 1 means tagEvent, join with likeStream   
        var eventType = "tagEvent";
        bool tagEvent = true;
        var streamToJoin = likeStreamBuffer;

        if (sourceID == 1) { // tagEvent
            tagStreamBuffer.Add(e);                        
        }
        else { // likeEvent
            likeStreamBuffer.Add(e);
            tagEvent = false;
            streamToJoin = tagStreamBuffer;                    
            eventType = "likeEvent";
        }

        // Console.WriteLine();
        // Console.WriteLine("========================================================================================");
        // Console.WriteLine($"[New Event] {eventType}, time = {e.timestamp}, content = {Event.GetContent<Tuple<int, int>>(e)}");

        // Console.WriteLine();
        // foreach (var e1 in streamToJoin) {
        //     Console.WriteLine($"streamToJoin: {e1.timestamp}, {Event.GetContent<Tuple<int, int>>(e1)}");
        // }
        
        // get the substream that timestamp is between (e.timestamp - windowLength, e.timestamp + windowLength) use stream
        // eg. window instance [0, 6) , 0 and 6 are not included in same window instance
        var subStream = streamToJoin.Where(x => x.timestamp > e.timestamp - windowLength && x.timestamp < e.timestamp + windowLength);

        // Console.WriteLine();
        // foreach (var e1 in subStream) {
        //     Console.WriteLine($"subStream: {e1.timestamp}, content = {Event.GetContent<Tuple<int, int>>(e1)}");
        // }

        // get the window instances that the event belongs to
        var e_WindowInstances = getWindowInstances(e.timestamp, windowSlide, windowLength);

        // Console.WriteLine();
        // Console.Write($"e_WindowInstances: ");
        // foreach (var e1 in e_WindowInstances) {
        //     Console.Write($" {e1} , ");
        // }
        // Console.WriteLine();

        int ii=0;
        // iterate through the substream
        foreach (var eventToJoin in subStream) {
            var eventToJoin_WindowInstances = getWindowInstances(eventToJoin.timestamp, windowSlide, windowLength);
            // Console.WriteLine();
            // Console.WriteLine($"**********[Candidate] {eventToJoin.timestamp}, {Event.GetContent<Tuple<int, int>>(eventToJoin)}**********");            
            // Console.WriteLine($"Candidate_WindowInstances: ");
            // foreach (var e1 in eventToJoin_WindowInstances) {
            //     Console.Write($" {e1} , ");                
            // }
            // Console.WriteLine();
            
            // get the intersection of the two window instances
            var intersection = e_WindowInstances.Intersect(eventToJoin_WindowInstances);
            int j = 0;
            while (j < intersection.Count()) {
                // Console.Write($"intersection[{j}] = {intersection.ElementAt(j)}   ");
                // do the join
                long newTimestamp = intersection.ElementAt(j) + windowLength - 1; // as we use the start timestamp of the window instance to represent the window instance id
                var e1 = tagEvent ? e : eventToJoin;
                var e2 = tagEvent ? eventToJoin : e;                
                Event joinedResult = Functions.WindowJoin(newTimestamp, e1, e2);   
                // if (joinedResult == null) Console.WriteLine($"joinedResult = null");                                                         
                // send the result to the output stream
                if (joinedResult != null) {
                    // Console.WriteLine($"output: ts = {joinedResult.timestamp}");
                    // Console.WriteLine($" ****** send event ******* {newTimestamp}, {Event.GetContent<Tuple<long, long, int, int>>(joinedResult)}");
                    await outputStream.OnNextAsync(joinedResult);                                                
                }
                j++;
            }             
        }

    }

    // for a single event, find all the window instances that it belongs to
    public static List<long> getWindowInstances(long eventTimestamp, int windowSlide, int windowLength)
    {
        var initialTime = Constants.initialWatermark;
        List<long> windowInstances = new List<long>();

        // event may be in multiple window instance, eg : 4  in [0,6) and [3,9)
        
        // the offset of the event from the start of the window, eg : offset = 4 % 3 = 1
        long offset = eventTimestamp % windowSlide;
        // Note negative value, eg : eventTimestamp = -2 in [-3, 3) , offset = -2 % 3 = -2
        if (eventTimestamp < 0) offset = windowSlide + offset;

        // use startWindow to represent the window instance id, eg 3 to represent [3, 9)
        long windowID = eventTimestamp - offset; // eg : 4 - 1 = 3, means 4 is in [3, 9)
        windowInstances.Add(windowID);
                
        // check smaller windows
        long tmpID = windowID - windowSlide; // eg : 3 - 3 = 0, means check [0, 6)
        while(eventTimestamp >= tmpID && eventTimestamp < tmpID + windowLength && tmpID >= initialTime) {
            windowInstances.Add(tmpID);
            tmpID -= windowSlide;                
        }
        
        // check larger windows
        tmpID = windowID + windowSlide; // eg : 3 + 3 = 6, means check [6, 12)
        while(eventTimestamp >= tmpID && eventTimestamp < tmpID + windowLength) {
            windowInstances.Add(tmpID);
            tmpID += windowSlide;                
        }            

        return windowInstances;
    }
}