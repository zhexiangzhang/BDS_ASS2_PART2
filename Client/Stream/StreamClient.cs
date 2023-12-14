using Library.Stream;
using Utilities;
using System.Diagnostics;
using Orleans.Streams;
using Orleans.Runtime;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Client.Stream;

internal class StreamClient
{
    // If true, read data from given datasets, otherwise, generate data on-the-fly
    static bool useTestData = true;
    static TimeSpan maxRunTime = TimeSpan.FromSeconds(120);

    // If true, the source data will also be published to Kafka channels
    // Notice: need to turn on the Kafka container first
    static bool useKafka = false;
    
    static int randSpan = 100;
    static IClusterClient client;
    static IStreamProvider streamProvider;
    static int windowSlide = 5000;    // in ms
    static int windowLength = 15000;  // in ms
   
    public async Task RunClient()
    {
        List<string> dataSets;
        if (useTestData) dataSets = new List<string> { @"SampleDataSet\InOrderStream\", @"SampleDataSet\OutOfOrderStream\", @"DataSet\InOrderStream\", @"DataSet\OutOfOrderStream\" };
        else dataSets = new List<string> { @"RandomStream\" };

        foreach (var filename in dataSets)
        {
            Console.WriteLine();
            Console.WriteLine($"[Q] Ready to process dataset {filename}? Press Enter to start...");
            Console.ReadLine();

            // ================================================================================================================
            // STEP 0: get stream references for three streams
            client = await OrleansClientManager.GetClient();
            streamProvider = client.GetStreamProvider(Constants.defaultStreamProvider);
            var tagStream = streamProvider.GetStream<Event>(StreamId.Create("tag", Guid.NewGuid()));
            var likeStream = streamProvider.GetStream<Event>(StreamId.Create("like", Guid.NewGuid()));

            // ================================================================================================================
            // STEP 1: set up one KafkaConnector for each stream
            // so the stream data is also published to kafka channel
            // those kafka channels are subscribed by Flink program
            if (useKafka) await SetUpKafkaConnectors(tagStream, likeStream);

            // ================================================================================================================
            // STEP 2: set file path and clear the stale content in the result file
            var dataDirectory = Constants.dataPath + filename;
            var resultFile = dataDirectory + "OrleansResult.txt";
            if (!Directory.Exists(dataDirectory)) Directory.CreateDirectory(dataDirectory);
            if (File.Exists(resultFile)) File.Delete(resultFile);

            // ================================================================================================================
            // STEP 3: build up the operator topologies for your query
            await BuildQueryTopology(tagStream, likeStream, resultFile);

            // ================================================================================================================
            // STEP 4: set and start the workload generator
            WorkloadGenerator workload;
            if (useTestData) workload = new WorkloadGenerator(dataDirectory);
            else workload = new WorkloadGenerator(randSpan, maxRunTime);
            if (useKafka)
            {
                Console.WriteLine();
                Console.WriteLine($"[Q] Start the Flink program now, then press Enter to continue...");
                Console.ReadLine();
                Console.WriteLine($"start timestamp: {DateTime.Now}");
            }
            await workload.Run(tagStream, likeStream);

            // ================================================================================================================
            // STEP 5: check the correctness of query results
            if (useTestData)
            {
                Console.WriteLine($"[Q] Is the query finished? Press Enter to check the correctness of query result...");
                Console.ReadLine();
                CheckCorrectness(dataDirectory, resultFile);
            }
        }
    }

    async Task SetUpKafkaConnectors(IAsyncStream<Event> tagStream,
                                    IAsyncStream<Event> likeStream)
    {
        // delete all topics so to clean up stale unprocessed events in kafka channels
        // the "query-result" is used when the Flink program publishes query result to a kafka channel"
        var topics = new List<string> { "tag", "like", "query-result" };
        var kafkaClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Constants.kafkaService, ClientId = "Orleans" }).Build();
        foreach (var topic in topics)
        {
            try
            {
                // may throw exception if the topic does not exist
                await kafkaClient.DeleteTopicsAsync(new List<string> { topic });
                Console.WriteLine($"Kafka topic {topic} is deleted, wait for 5s...");
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
            catch (Exception)
            {
                // ignore the exception and continue
            }

            await kafkaClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = topic, NumPartitions = 1 } });
            Console.WriteLine($"Kafka topic {topic} is re-created. ");
        }
        kafkaClient.Dispose();
        
        // create connector actors to transfer received events to kafka channels
        var tagKafkaConnector = client.GetGrain<IKafkaConnector>("tag");
        var likeKafkaConnector = client.GetGrain<IKafkaConnector>("like");

        await tagKafkaConnector.Init(tagStream, "tag");
        await likeKafkaConnector.Init(likeStream, "like");
        Console.WriteLine($"Three kafka connectors are set up. ");
    }

    async Task BuildQueryTopology(IAsyncStream<Event> tagStream,
                                  IAsyncStream<Event> likeStream,
                                  string resultFile)
    {
        // set up one windowed join operator with two source streams
        var join = client.GetGrain<IWindowJoinOperator>("join");
        var joinedResultStream = streamProvider.GetStream<Event>(StreamId.Create("joinedResult", Guid.NewGuid()));
        await join.Init(tagStream, likeStream, joinedResultStream, windowSlide, windowLength);

        // set up one filter operator with one source stream
        var filter = client.GetGrain<IFilterOperator>("filter");
        var filteredStream = streamProvider.GetStream<Event>(StreamId.Create("filteredResult", Guid.NewGuid()));
        await filter.Init(joinedResultStream, filteredStream);

        // set up one windowed aggregate operator with one source stream
        var aggregate = client.GetGrain<IWindowAggregateOperator>("aggregate");
        var aggregatedStream = streamProvider.GetStream<Event>(StreamId.Create("aggregatedResult", Guid.NewGuid()));
        await aggregate.Init(filteredStream, aggregatedStream, windowSlide, windowLength);

        // set up one sink operator to write the result to a local file
        var sink = client.GetGrain<ISinkOperator>("sink");
        await sink.Init(aggregatedStream, resultFile);

        Console.WriteLine($"The query topology is built. ");
    }

    void CheckCorrectness(string dataDirectory, string resultFile)
    {
        Console.WriteLine("Check query result...");

        // STEP 1: read the expected result
        var expected = new List<Tuple<int, int>>();
        using (var file = new StreamReader(dataDirectory + @"..\expected_result.txt"))
        { 
            var line = file.ReadLine();
            while (line != null)
            {
                var strs = line.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                expected.Add(new Tuple<int, int>(int.Parse(strs[0]), int.Parse(strs[1])));
                line = file.ReadLine();
            }
        }
        expected.Sort();

        // STEP 2: read the actual result
        var actual = new List<Tuple<int, int>>();
        using (var file = new StreamReader(resultFile))
        {
            var line = file.ReadLine();
            while (line != null)
            {
                var strs = line.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                actual.Add(new Tuple<int, int>(int.Parse(strs[0]), int.Parse(strs[1])));
                line = file.ReadLine();
            }
        }
        actual.Sort();

        // STEP 3: check if all records in the actual result are also in the expected result
        Debug.Assert(actual.Count == expected.Count);
        for (int i = 0; i < actual.Count; i++)
        {
            Console.WriteLine($"exp: <{expected[i].Item1}, {expected[i].Item2}>, act: <{actual[i].Item1}, {actual[i].Item2}>");
            Debug.Assert(actual[i].Item1 == expected[i].Item1 && actual[i].Item2 == expected[i].Item2);
        } 
        Console.WriteLine("The query result is corret. ");
    }
}