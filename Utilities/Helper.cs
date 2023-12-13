namespace Utilities;

public static class Helper
{
    // get the corresponding window instance ID for a given timestamp
    // eg, ts = 3000, falls in window [0, 5k), windowInstanceID = 0
    // eg, ts = 6000, falls in window [5k, 10k), windowInstanceID = 5k
    public static long GetWindowInstanceID(long timestamp, int windowSlide)
        => (timestamp / windowSlide) * windowSlide;
}