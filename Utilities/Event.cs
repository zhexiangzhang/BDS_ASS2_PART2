using Confluent.Kafka;
using MessagePack;

namespace Utilities;

[GenerateSerializer]
[MessagePackObject]
public sealed class Event
{
    [Key(0)]
    [Id(0)]
    public readonly long timestamp;
    [Key(1)]
    [Id(1)]
    public readonly EventType type;
    [Key(2)]
    [Id(2)]
    public readonly byte[] content;

    public Event(long timestamp, EventType type, byte[] content)
    {
        this.timestamp = timestamp;
        this.type = type;
        this.content = content;
    }

    public static Event CreateEvent<T>(long timestamp, EventType type, T data)
    {
        try
        {
            var content = MessagePackSerializer.Serialize(data);
            //Console.WriteLine($"CreateEvent: data = {data}, type = {type}, content size = {content.Length}");
            return new Event(timestamp, type, content);
        }
        catch (MessagePackSerializationException e)
        {
            Console.WriteLine($"Fail to serialize data, {e.Message}, {e.StackTrace}");
            throw;
        }
    }

    public static T GetContent<T>(Event eve)
    {
        try
        {
            var data = MessagePackSerializer.Deserialize<T>(eve.content);
            return data;
        }
        catch (MessagePackSerializationException e)
        {
            Console.WriteLine($"Fail to deserialize data, {e.Message}, {e.StackTrace}");
            throw;
        }
    }
}

public class EventSerializer : ISerializer<Event>, IDeserializer<Event>
{
    public byte[] Serialize(Event e, SerializationContext _)
    {
        var data = MessagePackSerializer.Serialize(e);
        //Console.WriteLine($"Serialize event: ts = {e.timestamp}, type = {e.type}, content size = {e.content.Length}, total size = {data.Length}");
        return data;
    }

    public Event Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext _)
    {
        if (isNull) return null;
        var e = MessagePackSerializer.Deserialize<Event>(data.ToArray());
        //Console.WriteLine($"Deserialize event: total size = {data.Length}, ts = {e.timestamp}, type = {e.type}, content size = {e.content.Length}");
        return e;
    }
}