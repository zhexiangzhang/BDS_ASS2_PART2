using System.Diagnostics;
using Utilities;
using Orleans.Streams;

namespace Client.Stream;

internal class WorkloadGenerator
{
    readonly int numUser = 1000;
    readonly int numPhoto = 10000;
    readonly Random rnd;
    readonly int randSpan;    // time span for timestamp randomization, used for generating out-of-order timestamps
    readonly TimeSpan runTime;
    readonly int watermarkSpeed;
    CountdownEvent allThreadsStart;
    CountdownEvent allThreadsEnd;

    readonly bool useTestData;
    readonly string dataDirectory;

    List<Tuple<int, int>> tagEvents;     // <photoID, userID>
    List<Tuple<int, int>> likeEvents;    // <userID, photoID>

    public WorkloadGenerator(string dataDirectory)   // read test data from local file
    {
        Init();
        useTestData = true;
        this.dataDirectory = dataDirectory;
    }

    public WorkloadGenerator(int randSpan, TimeSpan runTime)
    {
        Init();
        useTestData = false;
        rnd = new Random();
        Debug.Assert(randSpan >= 0);
        this.randSpan = randSpan;
        this.runTime = runTime;
        watermarkSpeed = 100;      // emit one watermark for every 100 events

        // generate all possible users and photos
        var users = new int[numUser];
        var photos = new int[numPhoto];
        for (int i = 0; i < numUser; i++) users[i] = i;
        for (int i = 0; i < numPhoto; i++) photos[i] = i;
        // shuffle the list
        var newPhotos = photos.OrderBy(i => rnd.Next()).ToArray();
        var newUsers = users.OrderBy(j => rnd.Next()).ToArray();

        // generate all possible events
        tagEvents = new List<Tuple<int, int>>();
        likeEvents = new List<Tuple<int, int>>();
        for (int i = 0; i < numPhoto; i++)
        {
            // randomly select a group of users to build relation with the photo
            var minUser = rnd.Next(0, numUser / 2);
            var maxuser = rnd.Next(numUser / 2, numUser);
            for (int j = minUser; j < maxuser; j++)
            {
                tagEvents.Add(new Tuple<int, int>(newPhotos[i], newUsers[j]));
                likeEvents.Add(new Tuple<int, int>(newUsers[j], newPhotos[i]));
            }
        }
    }

    void Init()
    {
        allThreadsStart = new CountdownEvent(2);
        allThreadsEnd = new CountdownEvent(2);
    }

    public async Task Run(IAsyncStream<Event> tagStream,
                          IAsyncStream<Event> likeStream)
    {
        Thread t1;
        Thread t2;
        if (useTestData)
        {
            t1 = new Thread(GenerateStreamFromFile);
            t2 = new Thread(GenerateStreamFromFile);
        }
        else
        {
            t1 = new Thread(GenerateStream);
            t2 = new Thread(GenerateStream);
        }

        var start = DateTime.Now;
        t1.Start(new Tuple<string, IAsyncStream<Event>>("tag", tagStream));
        t2.Start(new Tuple<string, IAsyncStream<Event>>("like", likeStream));
        allThreadsEnd.Wait();
        Console.WriteLine($"WorkloadGenerator finish running for {(int)((DateTime.Now - start).TotalSeconds)}s");
    }

    async void GenerateStreamFromFile(object obj)
    {
        var input = (Tuple<string, IAsyncStream<Event>>)obj;
        var type = input.Item1;
        var stream = input.Item2;
        var watch = new Stopwatch();
        
        allThreadsStart.Signal();
        allThreadsStart.Wait();
        watch.Start();
        Console.WriteLine($"Start to read data from file for {type} stream...");

        var filePath = dataDirectory + $"{type}.txt";
        using (var file = new StreamReader(filePath))
        {
            var line = file.ReadLine();
            while (line != null)
            {
                var strs = line.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                var timestamp = long.Parse(strs[0]);
                if (strs.Length == 1)    // this is a watermark
                {
                    var watermark = Event.CreateEvent(timestamp, EventType.Watermark, new byte[0]);
                    await stream.OnNextAsync(watermark);
                }
                else
                {
                    var content = new Tuple<int, int>(int.Parse(strs[1]), int.Parse(strs[2]));
                    var e = Event.CreateEvent(timestamp, EventType.Regular, content);
                    await stream.OnNextAsync(e);
                }
                line = file.ReadLine();
            }
        }
        allThreadsEnd.Signal();
    }

    async void GenerateStream(object obj)
    {
        var input = (Tuple<string, IAsyncStream<Event>>)obj;
        var type = input.Item1;
        var stream = input.Item2;
        var watch = new Stopwatch();
        var count = 0;
        var lastEventTimestamp = long.MinValue;

        allThreadsStart.Signal();
        allThreadsStart.Wait();
        watch.Start();
        Console.WriteLine($"Start to generate data for {type} stream...");
        while (watch.Elapsed < runTime)
        {
            // generate an event
            Event e;
            if (type == "tag")
            {
                if (tagEvents.Count == 0) break;
                e = GenerateTagEvent();
            }
            else if (type == "like")
            {
                if (likeEvents.Count == 0) break;
                e = GenerateLikeEvent();
            } 
            else throw new Exception($"Exception: stream type {type} is not supported. ");

            await stream.OnNextAsync(e);

            // generate a watermark
            if (count != 0 && count % watermarkSpeed == 0)
            {
                var watermark = Event.CreateEvent(GetWatermark(e.timestamp), EventType.Watermark, new Tuple<int, int>(-1, -1));
                await stream.OnNextAsync(watermark);
            }
            count++;
            lastEventTimestamp = e.timestamp;
        }
        // emit one more watermark for all the rests of events
        var wm = Event.CreateEvent(lastEventTimestamp + 100000, EventType.Watermark, new Tuple<int, int>(-1, -1));
        Console.WriteLine($"Emit last watermark to {type} stream, timestamp = {wm.timestamp}");
        await stream.OnNextAsync(wm);

        allThreadsEnd.Signal();
    }

    Event GenerateTagEvent()     // photo tags a user
    {
        var timestamp = GenerateTimestamp();
        var tagInfo = tagEvents.First();
        tagEvents.RemoveAt(0);
        return Event.CreateEvent(timestamp, EventType.Regular, tagInfo);
    }

    Event GenerateLikeEvent()    // user likes a photo
    {
        var timestamp = GenerateTimestamp();
        var likeInfo = likeEvents.First();
        likeEvents.RemoveAt(0);
        return Event.CreateEvent(timestamp, EventType.Regular, likeInfo);
    }

    long GenerateTimestamp()
    {
        var time = (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
        return time + rnd.Next(2 * randSpan + 1) - randSpan;
    }

    long GetWatermark(long timestamp)
    {
        return timestamp - 2 * randSpan - 1 ;
    }
}