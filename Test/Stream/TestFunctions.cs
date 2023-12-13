using System.Diagnostics;
using SocialNetwork;
using Utilities;

namespace Test.Stream;

[TestClass]
public class TestFunctions
{
    [TestMethod]
    public void TestFilterExample()
    {
        var rnd = new Random();
        for (int i = 0; i < 1000; i++)
        {
            var number = rnd.Next(0, 100);
            var expected = number < 50;
            var e = Event.CreateEvent(0, EventType.Regular, number);
            var actual = Functions.FilterExample(e);
            Debug.Assert(expected == actual);
        }
    }

    [TestMethod]
    public void TestFilter()
    {
        
    }

    [TestMethod]
    public void TestWindowAggregator()
    {
        // prepare some test data (maybe 10 - 20 events)

        // calculate the expected output by hand

        // run the WindowAggregator() function

        // check if the actual output matches the expectation
    }

    [TestMethod]
    public void TestWindowJoin()
    {
    }
}