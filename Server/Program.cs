using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Utilities;

var builder = new HostBuilder()
        .UseOrleans(siloBuilder =>
        {
            siloBuilder
                .UseLocalhostClustering()                         // the silo membership table will be maintained in-memory
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Constants.ClusterId;
                    options.ServiceId = Constants.ServiceId;
                })
                .Configure<EndpointOptions>(options =>
                {
                    options.SiloPort = Constants.SiloPort;        // silo-to-silo communication
                    options.GatewayPort = Constants.GatewayPort;  // client-to-silo communication
                })
                .AddMemoryStreams(Constants.defaultStreamProvider, options =>
                {
                    options.ConfigureStreamPubSub(Orleans.Streams.StreamPubSubType.ExplicitGrainBasedAndImplicit);
                    //options.FireAndForgetDelivery = Constants.FireAndForgetDelivery;
                })
                .AddMemoryGrainStorage(Constants.defaultStreamStorage)
                .UseDashboard(options => { });    // localhost:8080
                //.ConfigureLogging(loggingBuilder => loggingBuilder.AddConsole()); // enable this logging to check Orleans internal info
        });

var server = builder.Build();
await server.StartAsync();
Console.WriteLine("\n *************************************************************************");
Console.WriteLine("   The BDS Social Network server is started. Press Enter to terminate...    ");
Console.WriteLine("\n *************************************************************************");
Console.ReadLine();
Console.WriteLine();
Console.WriteLine("   Terminating server...");
await server.StopAsync();