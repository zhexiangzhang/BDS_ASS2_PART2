namespace Utilities;

public enum EventType {Regular, Watermark};

public class Constants
{
    public const int SiloPort = 11111;
    public const int GatewayPort = 30000;
    public const string ClusterId = "LocalTestCluster";
    public const string ServiceId = "BDSSocialNetwork";

    public const long initialWatermark = -100000;

    public const string defaultStreamStorage = "PubSubStore";
    public const string defaultStreamProvider = "SMSProvider";
    public const string kafkaService = "localhost:19092";

    public const string dataPath = @"..\....\..\Data\";
    // public const string dataPath = @"E:\KU\1\BDS\Assignment\ASS2_Part2\BDS-Programming-Assignment-2-1\BDS-Programming-Assignment-2\Data\";
}