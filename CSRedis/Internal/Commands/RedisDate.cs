using Redis.NET.Internal.IO;
using System;

namespace Redis.NET.Internal.Commands
{
    class RedisDate : RedisCommand<DateTime>
    {
        static readonly DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public RedisDate(string command, params object[] args)
            : base(command, args)
        { }

        public override DateTime Parse(RedisReader reader) => FromTimestamp(reader.ReadInt());

        public class Micro : RedisCommand<DateTime>
        {
            public Micro(string command, params object[] args)
                : base(command, args)
            { }

            public override DateTime Parse(RedisReader reader)
            {
                reader.ExpectType(RedisMessage.MultiBulk);
                reader.ExpectSize(2);

                var timestamp = int.Parse(reader.ReadBulkString());
                var microseconds = int.Parse(reader.ReadBulkString());

                return FromTimestamp(timestamp, microseconds);
            }

            public static DateTime FromTimestamp(long timestamp, long microseconds) => RedisDate.FromTimestamp(timestamp) + FromMicroseconds(microseconds);

            public static TimeSpan FromMicroseconds(long microseconds) => TimeSpan.FromTicks(microseconds * (TimeSpan.TicksPerMillisecond / 1000));

            public static long ToMicroseconds(TimeSpan span) => span.Ticks / (TimeSpan.TicksPerMillisecond / 1000);
        }

        public static DateTime FromTimestamp(long seconds) => _epoch + TimeSpan.FromSeconds(seconds);

        public static TimeSpan ToTimestamp(DateTime date) => date - _epoch;
    }
}