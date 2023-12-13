using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Streams;
using Utilities;

namespace Client;

public static class OrleansClientManager
{
    public static async Task<IClusterClient> GetClient()
    {
        var host = new HostBuilder()
            .UseOrleansClient(builder =>
            {
                builder.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Constants.ClusterId;
                    options.ServiceId = Constants.ServiceId;
                });

                builder.AddMemoryStreams(Constants.defaultStreamProvider, options =>
                {
                    options.ConfigureStreamPubSub(StreamPubSubType.ExplicitGrainBasedAndImplicit);
                    //options.FireAndForgetDelivery = Constants.FireAndForgetDelivery;
                });

                builder.UseLocalhostClustering();
            }).Build();

        await host.StartAsync();
        return host.Services.GetRequiredService<IClusterClient>();
    }
}