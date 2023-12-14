using Confluent.Kafka;
using Utilities;

namespace SocialNetwork;

public static class Functions
{
    public static Func<Event, bool> FilterExample = e =>
    {
        var number = BitConverter.ToInt32(e.content);
        return number < 50;
    };

    public static Func<long, Event, Event, Event> WindowJoin = (timestamp, e1, e2) =>
    {
        var tagContent = Event.GetContent<Tuple<int, int>>(e1);
        var likeContent = Event.GetContent<Tuple<int, int>>(e2);
        //Tag <timestamp, photoID, userID>
        //Like <timestamp, userID, photoID>
        bool photoIDMatch = tagContent.Item1 == likeContent.Item2;
        bool userIDMatch = tagContent.Item2 == likeContent.Item1;        
        if (!photoIDMatch || !userIDMatch) return null;
        var content =  Tuple.Create(e1.timestamp, e2.timestamp, tagContent.Item1, tagContent.Item2);
        var resEvent = Event.CreateEvent(timestamp, EventType.Regular, content);
        return resEvent;
    };

    public static Func<Event, bool> Filter = e =>
    {
        // <ts: photo tagged the user, ts: user liked the photo, photo ID, user ID>
        var joinedResult = Event.GetContent<Tuple<long, long, int, int>>(e);
        return joinedResult.Item1 < joinedResult.Item2;   // user likes the photo after he/she is tagged
    };

    public static Func<long, List<Event>, List<Event>> WindowAggregator = (timestamp, events) =>
    {
        // loop through events and count likes that each photo received
        var photoLikes = new Dictionary<int, MyCounter>();
        foreach (var e in events)
        {
            var content = Event.GetContent<Tuple<long, long, int, int>>(e);
            var photoID = content.Item3;
            if (!photoLikes.ContainsKey(photoID))
            {
                photoLikes[photoID] = new MyCounter();
            }
            photoLikes[photoID].Increment();
        }
        // create output events
        var outputEvents = new List<Event>();
        foreach (var photoID in photoLikes.Keys)
        {
            var count = photoLikes[photoID].Get();
            var content = new Tuple<int, int>(photoID, count);                    
            var outputEvent = Event.CreateEvent(timestamp, EventType.Regular, content);            
            outputEvents.Add(outputEvent);
        }
        return outputEvents;
    };

    public static Func<string, Event, Null> Sink = (resultFile, e) =>
    {
        if (e.type == EventType.Regular)
        {
            using (var file = new StreamWriter(resultFile, true))
            {
                var content = Event.GetContent<Tuple<int, int>>(e);
                Console.WriteLine($"output: ts = {e.timestamp}, photoID = {content.Item1}, count = {content.Item2}");
                file.WriteLine($"{content.Item1} {content.Item2}");                
            }
        }
        return null;
    };
}

internal class MyCounter
{
    int n;

    public MyCounter() => n = 0;

    public void Increment() => n++;

    public int Get() => n;    
}